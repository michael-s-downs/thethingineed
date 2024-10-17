### This code is property of the GGAO ###


# Native import
import io
import time
from typing import Tuple
from statistics import mean

# Installed import
import cv2
import numpy as np
from google.cloud import vision
from google.cloud.vision import types
from google.oauth2 import service_account


# compute the most common value in a list (it takes the smallest of the two most commons)
def mode(lst: list) -> int:
    """Computes the most common value in a list

    :param lst: (list) list of values
    :return: (int) the most common value
    """
    lst = sorted(set(lst), key=lst.count)

    if abs(lst[0]) < 45:
        return lst[0]
    else:
        return lst[1]


def runOCR(credentials: dict, paths: list) -> list:
    """ Runs google ocr

    :param credentials: (dict) google credentials
    :param paths: (list) list of paths to the images
    :return: (list) the document and the angles
    """
    client_options = {'api_endpoint': "eu-vision.googleapis.com"}
    client = vision.ImageAnnotatorClient(credentials=credentials, client_options=client_options)

    requests = []

    for path in paths:
        with io.open(path, "rb") as image_file:
            image = types.Image(content=image_file.read())
            feature = types.Feature(type=vision.enums.Feature.Type.DOCUMENT_TEXT_DETECTION)
            request = types.AnnotateImageRequest(image=image, features=[feature])
            requests.append(request)

    response = client.batch_annotate_images(requests)

    features = []

    for element in response.responses:
        document = element.full_text_annotation
        angles = []
        for page in document.pages:
            for block in page.blocks:
                vertices = block.bounding_box.vertices
                a, b = (vertices[1].x - vertices[0].x, vertices[1].y - vertices[0].y)
                new_angle = np.angle(a + b * 1j, deg=True)
                for paragraph in block.paragraphs:
                    angles = angles + [new_angle] * len(paragraph.words)

        features.append((document, angles))

    return features


def read_blocks(document, ppal_rotation: list, ppal_angle: float, d_angle: float) -> list:
    """Selects only the blocks with rotation after applying ppal_rotation is lower than d_angle

    :param document: google OCR output
    :param ppal_rotation: (np.array) straightening matrix
    :param ppal_angle: (float) principal angle
    :param d_angle: (float)  max angle distorsion #  d_angle [tuple of float] --  min and max angle distorsion
    :return: (list) those blocks according to the rotation and min/max distorsion
    """
    blocks = []
    for page in document.pages:
        for block in page.blocks:

            block_angle, center, vertices = straighten_text(
                ppal_rotation, block.bounding_box.vertices
            )

            if abs(block_angle) < d_angle:
                block_rotation = cv2.getRotationMatrix2D(center, block_angle, 1)
                block_rotation = np.dot(block_rotation, [ppal_rotation[0], ppal_rotation[1], [0, 0, 1]])
            else:
                continue

            for paragraph in block.paragraphs:
                for word in paragraph.words:

                    if ppal_angle != 0:
                        word_angle, center, vertices = straighten_text(
                            block_rotation, word.bounding_box.vertices)

                        word_rotation = cv2.getRotationMatrix2D(center, word_angle, 1)
                        vertices = [np.dot(word_rotation, [v[0], v[1], 1]) for v in vertices]
                    else:
                        vertices = [[v.x, v.y] for v in word.bounding_box.vertices]

                    block_text = ""
                    for symbol in word.symbols:
                        if symbol.property.detected_break.type != 0:
                            block_text = block_text + symbol.text + " "
                        else:
                            block_text = block_text + symbol.text

                    X = [v[0] for v in vertices]
                    Y = [v[1] for v in vertices]
                    l = int(min(X))
                    r = int(max(X))
                    t = int(min(Y))
                    b = int(max(Y))
                    aux = [t, b, l, r, block_text, word.confidence]

                    blocks.append(aux)

    return blocks


def get_distorsion_angle(angles: list, ppal_angle: float, d_angle: float) -> float:
    """ Get the max and min distance from ppal_angle which has not a gap bigger than d_angle

    :param angles: (list) list of angles
    :param ppal_angle: (float) principal angle
    :param d_angle: (float) max angle distorsion
    :return: (float) the max and min distance from ppal_angle which has not a gap bigger than d_angle
    """
    nogap_angles = []
    nogap_angles2 = [ a for a in angles
        if abs(min(a - ppal_angle, a - ppal_angle + 360) % 360) < d_angle]

    d_angle2 = d_angle

    while len(nogap_angles) != len(nogap_angles2):
        nogap_angles = nogap_angles2.copy()
        d_angle2 = d_angle + d_angle2
        nogap_angles2 = [ a for a in angles
            if abs(min(a - ppal_angle, a - ppal_angle + 360) % 360) < d_angle2]

    return d_angle2


def detect_document(credentials: dict, paths: list, expansion: float, rotated: float, simple: bool = False, distorsion_angle: int = 10) -> list:
    """ Reads a document with google vision, takes the angle of the image and takes the word positions joining
    some of them if they are too near

    :param credentials: (dict) google credentials
    :param paths: (list) the local filepaths of the image
    :param expansion: (float) the expansion of the image
    :param rotated: (bool) if the document is rotated
    :param simple: (bool) if the document is simple
    :param distorsion_angle: (float) angle in degrees which represents the posible deformation of the document
    :return: (list)
        blocks [list of list] -- list of the top botton left right and text of each word
        wicth, height [int] -- dimensions of the image
        font_size [int] -- mean of the height of the words
        min_size [int] -- minimun word height
        rotation [nupmy array] -- the 2D transformation matrix which
    """
    detections = []

    features = runOCR(credentials, paths)

    for document, angles in features:
        width, height = (document.pages[0].width, document.pages[0].height)

        # most common angle is the document principal angle
        if rotated:
            ppal_angle = mode(angles)
        else:
            ppal_angle = 0
            distorsion_angle = 45
        ppal_rotation = cv2.getRotationMatrix2D((width / 2, height / 2), ppal_angle, 1)

        # get the min and max angles for ppal_angle and the blocks between them
        d_angle2 = get_distorsion_angle(angles, ppal_angle, distorsion_angle)

        blocks = read_blocks(document, ppal_rotation, ppal_angle, d_angle2)

        sizes = [w[1] - w[0] for w in blocks]
        min_size = int(min(sizes))
        font_size = int(mean(sizes) + 1)

        if not simple:
            i = 0
            while i < len(blocks):

                for lettre in blocks[i][-1]:
                    if not ord(lettre) < 128:
                        pass
                    else:
                        break

                else:
                    blocks.pop(i)
                    continue

                j = i + 1

                while j < len(blocks):
                    # if a word is next to another one, join, bc they are the same phrase

                    h1 = blocks[i][1] - blocks[i][0]
                    h2 = blocks[j][1] - blocks[j][0]

                    d0 = h1 + h2  # /np.sqrt(2)

                    tr_1 = np.array([blocks[i][0], blocks[i][3]])
                    br_1 = np.array([blocks[i][1], blocks[i][3]])

                    tl_2 = np.array([blocks[j][0], blocks[j][2]])
                    bl_2 = np.array([blocks[j][1], blocks[j][2]])

                    d1 = np.linalg.norm(tr_1 - tl_2) + np.linalg.norm(br_1 - bl_2)

                    if d1 < d0:
                        st0 = min(blocks[i][0], blocks[j][0])
                        st1 = max(blocks[i][1], blocks[j][1])
                        st2 = min(blocks[i][2], blocks[j][2])
                        st3 = max(blocks[i][3], blocks[j][3])
                        st4 = blocks[i][4] + blocks[j][4]

                        blocks[i] = [st0, st1, st2, st3, st4]
                        blocks.pop(j)
                        continue

                    tr_2 = np.array([blocks[j][0], blocks[j][3]])
                    br_2 = np.array([blocks[j][1], blocks[j][3]])

                    tl_1 = np.array([blocks[i][0], blocks[i][2]])
                    bl_1 = np.array([blocks[i][1], blocks[i][2]])

                    d2 = np.linalg.norm(tl_1 - tr_2) + np.linalg.norm(bl_1 - br_2)

                    if d2 < d0:
                        st0 = min(blocks[i][0], blocks[j][0])
                        st1 = max(blocks[i][1], blocks[j][1])
                        st2 = min(blocks[i][2], blocks[j][2])
                        st3 = max(blocks[i][3], blocks[j][3])
                        st4 = blocks[j][4] + blocks[i][4]

                        blocks[i] = [st0, st1, st2, st3, st4]
                        blocks.pop(j)
                        continue
                    j += 1
                i += 1

            i = 0
            while i < len(blocks):
                j = i + 1
                while j < len(blocks):

                    h0 = min(blocks[i][1] - blocks[i][0], blocks[j][1] - blocks[j][0])

                    expanded_1 = np.array(blocks[i][:-1]) + np.array(
                        [-h0 * expansion, h0 * expansion, 0, 0])
                    expanded_2 = np.array(blocks[j][:-1]) + np.array(
                        [-h0 * expansion, h0 * expansion, 0, 0])

                    if expansion < 0:
                        blocks[i][:-1] = expanded_1
                        blocks[j][:-1] = expanded_2

                    if cuts_themselves(expanded_1, expanded_2):
                        st0 = min(blocks[i][0], blocks[j][0])
                        st1 = max(blocks[i][1], blocks[j][1])
                        st2 = min(blocks[i][2], blocks[j][2])
                        st3 = max(blocks[i][3], blocks[j][3])
                        st4 = blocks[i][4] + blocks[j][4]
                        blocks[i] = [st0, st1, st2, st3, st4]
                        blocks.pop(j)
                        continue

                    j += 1
                i += 1

        else:

            i = 0
            while i < len(blocks):
                j = i + 1
                while j < len(blocks):

                    h1 = blocks[i][1] - blocks[i][0]
                    h1 = int(h1 / 2 - 1 / 2)
                    h2 = blocks[j][1] - blocks[j][0]
                    h2 = int(h2 / 2 - 1 / 2)

                    expanded_1 = np.array(blocks[i][:-1]) + np.array([h1, -h1, 0, 0])
                    expanded_1[2:] = [0, width]

                    expanded_2 = np.array(blocks[j][:-1])

                    if expansion < 0:
                        blocks[i][:-1] = expanded_1
                        blocks[j][:-1] = expanded_2

                    if cuts_themselves(expanded_1, expanded_2):
                        st0 = min(blocks[i][0], blocks[j][0])
                        st1 = max(blocks[i][1], blocks[j][1])
                        st2 = min(blocks[i][2], blocks[j][2])
                        st3 = max(blocks[i][3], blocks[j][3])
                        st4 = blocks[i][4] + blocks[j][4]
                        blocks[i] = [st0, st1, st2, st3, st4]
                        blocks.pop(j)
                        continue

                    j += 1
                i += 1

        # fit the boxes to the superior left square of the image
        top, botton, left, right = (
            min([b[0] for b in blocks]),
            max([b[1] for b in blocks]),
            min([b[2] for b in blocks]),
            max([b[3] for b in blocks]),
        )

        blocks = [[b[0] - top, b[1] - top, b[2] - left, b[3] - left, b[4]] for b in blocks]

        width, height = (botton - top, right - left)

        detections.append((blocks, width, height, font_size, min_size, ppal_rotation, top, left))

    return detections


def get_blocks_cells(ocr_results: list) -> Tuple[list, list, list, list]:
    """ Get cells of blocks from an image

    :param ocr_results: (list) Results of runOCR
    :return: (tuple) List of results for the image (List of docs. Each doc list of pages. Each page list of cells).
            List<List<Cell>>
    """
    documents_blocks = []
    documents_paragraphs = []
    documents_words = []
    documents_lines = []
    for document, angles in ocr_results:
        pages_blocks = []
        pages_paragraphs = []
        pages_words = []
        pages_lines = []
        for i, page in enumerate(document.pages):
            cells = []
            words_cells = []
            lines_cells = []
            first_word = 0
            paragraphs_cells = []
            for block in page.blocks:
                block_lines = []
                for paragraph in block.paragraphs:
                    words = []
                    finish_line = False
                    for word in paragraph.words:
                        text = "".join([s.text for s in word.symbols])
                        r0 = min([v.y for v in word.bounding_box.vertices])
                        c0 = min([v.x for v in word.bounding_box.vertices])
                        r1 = max([v.y for v in word.bounding_box.vertices])
                        c1 = max([v.x for v in word.bounding_box.vertices])
                        rotation = np.arctan(np.abs(r1-r0)/np.abs(c1-c0))
                        rotation = 0
                        # if len(word.symbols) >= 2:
                        #     first_char = word.symbols[0]
                        #     last_char = word.symbols[-1]
                        #     first_char_center = (np.mean([v.x for v in first_char.bounding_box.vertices]),
                        #                          np.mean([v.y for v in first_char.bounding_box.vertices]))
                        #     last_char_center = (np.mean([v.x for v in last_char.bounding_box.vertices]),
                        #                         np.mean([v.y for v in last_char.bounding_box.vertices]))
                        #
                        #     # upright or upside down
                        #     if np.abs(first_char_center[1] - last_char_center[1]) < np.abs(r0 - r1):
                        #         if first_char_center[0] <= last_char_center[0]:  # upright
                        #             rotation = 0
                        #         else:  # updside down
                        #             rotation = 180
                        #     else:  # sideways
                        #         if first_char_center[1] <= last_char_center[1]:
                        #             rotation = 90
                        #         else:
                        #             rotation = 270
                        if word.bounding_box.vertices[0].x < word.bounding_box.vertices[2].x and word.bounding_box.vertices[0].y < word.bounding_box.vertices[2].y:
                            rotation = 0
                        elif word.bounding_box.vertices[0].x > word.bounding_box.vertices[2].x and word.bounding_box.vertices[0].y < word.bounding_box.vertices[2].y:
                            rotation = 90
                        elif word.bounding_box.vertices[0].x > word.bounding_box.vertices[2].x and word.bounding_box.vertices[0].y > word.bounding_box.vertices[2].y:
                            rotation = 180
                        elif word.bounding_box.vertices[0].x < word.bounding_box.vertices[2].x and word.bounding_box.vertices[0].y > word.bounding_box.vertices[2].y:
                            rotation = 270

                        words.append(text)
                        if word.symbols[-1].property.detected_break.type in [1, 2]:
                            words.append(" ")
                        elif word.symbols[-1].property.detected_break.type in [3, 5]:
                            words.append("\n")
                            finish_line = True
                        elif word.symbols[-1].property.detected_break.type in [4]:
                            words.append("-\n")
                            finish_line = True
                            text += "-"
                        words_cells.append(
                            {
                                'r0': r0,
                                'c0': c0,
                                'r1': r1,
                                'c1': c1,
                                'text': text,
                                'rotation': rotation,
                                'font': None,
                                'fontsize': None,
                                'confidence': word.confidence,
                                'page': i
                            }
                        )
                        if finish_line:

                            words_cells_line = words_cells[first_word:]
                            text_line = ""
                            for word_line in words_cells_line:
                                text_line += word_line['text'] +" "

                            r0 = min([v['r0'] for v in words_cells_line])
                            c0 = min([v['c0'] for v in words_cells_line])
                            r1 = max([v['r1'] for v in words_cells_line])
                            c1 = max([v['c1'] for v in words_cells_line])
                            lines_cells.append(
                                {
                                    'r0': r0,
                                    'c0': c0,
                                    'r1': r1,
                                    'c1': c1,
                                    'x_max': page.width,
                                    'y_max': page.height,
                                    'rotation': words_cells_line[0]['rotation'],
                                    'text': text_line,
                                    'font': None,
                                    'fontsize': None,
                                    'confidence': paragraph.confidence,
                                    'page': i
                                }
                            )
                            finish_line = False
                            first_word = len(words_cells)
                    text = "".join(words)
                    block_lines.append(text)
                    r0 = min([v.y for v in paragraph.bounding_box.vertices])
                    c0 = min([v.x for v in paragraph.bounding_box.vertices])
                    r1 = max([v.y for v in paragraph.bounding_box.vertices])
                    c1 = max([v.x for v in paragraph.bounding_box.vertices])
                    paragraphs_cells.append(
                        {
                            'r0': r0,
                            'c0': c0,
                            'r1': r1,
                            'c1': c1,
                            'text': text,
                            'font': None,
                            'fontsize': None,
                            'confidence': paragraph.confidence,
                            'page': i
                        }
                    )

                text = "".join(block_lines).strip()
                r0 = min([v.y for v in block.bounding_box.vertices])
                c0 = min([v.x for v in block.bounding_box.vertices])
                r1 = max([v.y for v in block.bounding_box.vertices])
                c1 = max([v.x for v in block.bounding_box.vertices])
                cells.append(
                    {
                        'r0': r0,
                        'c0': c0,
                        'r1': r1,
                        'c1': c1,
                        'text': text,
                        'font': None,
                        'fontsize': None,
                        'confidence': block.confidence,
                        'page': i
                    }
                )
            pages_blocks.append(cells)
            pages_words.append(words_cells)
            pages_paragraphs.append(paragraphs_cells)
            pages_lines.append(lines_cells)
        documents_blocks.append(pages_blocks)
        documents_words.append(pages_words)
        documents_paragraphs.append(pages_paragraphs)
        documents_lines.append(pages_lines)
    return documents_blocks, documents_paragraphs, documents_words, documents_lines


def get_words_cells(ocr_results: list, rotated: bool = False, distorsion_angle: int = 10):
    """ Get cells of words from path

    :param ocr_results: Results of runOCR
    :param rotated: If the document is rotated
    :param distorsion_angle: Angle of distorsion
    :return: (list) results for each image (List of docs. Each doc list of pages. Each page list of cells).
                List<List<Cell>>
    """
    documents = []
    for document, angles in ocr_results:
        width, height = (document.pages[0].width, document.pages[0].height)

        # most common angle is the document principal angle
        if rotated:
            ppal_angle = mode(angles)
        else:
            ppal_angle = 0
            distorsion_angle = 45
        ppal_rotation = cv2.getRotationMatrix2D((width / 2, height / 2), ppal_angle, 1)

        # get the min and max angles for ppal_angle and the blocks between them
        d_angle2 = get_distorsion_angle(angles, ppal_angle, distorsion_angle)

        blocks = read_blocks(document, ppal_rotation, ppal_angle, d_angle2)

        # fit the boxes to the superior left square of the image
        top, botton, left, right = (
            min([b[0] for b in blocks]),
            max([b[1] for b in blocks]),
            min([b[2] for b in blocks]),
            max([b[3] for b in blocks]),
        )

        blocks = [[b[0] - top, b[1] - top, b[2] - left, b[3] - left, b[4]] for b in blocks]
        cells = [
            {
                'r0': block[0],
                'c0': block[2],
                'r1': block[1],
                'c1': block[3],
                'text': block[4],
                'fontsize': block[1]-block[0],
                'font': None
            } for block in blocks]
        documents.append(cells)

    return documents


def get_blocks_cells_from_paths(credentials: dict, paths: list) -> list:
    """ Get cells of blocks from an image

    :param credentials: (dict) Credentials for the service account
    :param paths: (list) Path to images
    :return: (list) List of results for the image (List of docs. Each doc list of pages. Each page list of cells).
                List<List<Cell>>
    """
    results = runOCR(service_account.Credentials.from_service_account_info(credentials), paths)
    documents = get_blocks_cells(results)

    return documents


def get_words_cells_from_paths(credentials: dict, paths: list, rotated: bool = False, distorsion_angle: int = 10):
    """ Get cells of words from path

    :param credentials: (dict) Credentials for the service account
    :param paths: (list) Path to images
    :param rotated: (bool) true if doc is rotated
    :param distorsion_angle: (int) Angle of distorsion
    :return: (list) List of results for each image (List of docs. Each doc list of pages. Each page list of cells).
                List<List<Cell>>
    """
    results = runOCR(service_account.Credentials.from_service_account_info(credentials), paths)
    documents = get_words_cells(results, rotated, distorsion_angle)

    return documents


def straighten_text(rotation: list, vertices: list) -> Tuple[float, tuple, list]:
    """Rotates a boundering_box by a general rotation and computes the particular angle and center

    :param rotation: (list) 2D rotation and translation matrix
    :param vertices: (list) vertices of the boundering_box
    :return: (tuple)
        block_angle: (float) particular angle
        center: (tuple) particular center
        vertices: vertices of the boundering_box modified by rotation
    """

    vertices = [np.dot(rotation, [v.x, v.y, 1]) for v in vertices]
    a, b = (vertices[1][0] - vertices[0][0], vertices[1][1] - vertices[0][1])
    block_angle = np.angle(a + b * 1j, deg=True)
    center = (
        (vertices[0][0] + vertices[2][0]) / 2,
        (vertices[0][1] + vertices[2][1]) / 2,
    )

    return block_angle, center, vertices


def cuts_themselves(a: list, b: list) -> bool:
    """Check if two boxes cuts themselves

    :param a: (list) list of top, botton, left, and right value of each box
    :param b: (list) list of top, botton, left, and right value of each box
    :return: (bool) True if the boxes cuts, False if don´t
    """
    dx = min(a[1], b[1]) - max(a[0], b[0])
    dy = min(a[3], b[3]) - max(a[2], b[2])
    return (dx >= 0) and (dy >= 0)


def rebuild_original_blocks(blocks: list, rotation: list, top: int, left: int) -> list:
    """Fix the rotated and translated blocks with the original imagen

    :param blocks: (list) the blocks gives as top, botton, left, right and text
    :param rotation: (list) rotation matrix
    :param top: (int) Minimum top and left position of the original rotated but not translated blocks
    :param left: (int) Minimum top and left position of the original rotated but not translated blocks
    :return original_blocks: (list) -- blocks
    """

    original_blocks = []

    for bl in blocks:
        t, b, l, r = [bl[0] + top, bl[1] + top, bl[2] + left, bl[3] + left]
        t, b, l, r = bl

        # transform the 4 corners
        corners = np.array(
            [
                np.dot(rotation, [t, l, 1]),
                np.dot(rotation, [t, r, 1]),
                np.dot(rotation, [b, l, 1]),
                np.dot(rotation, [b, r, 1]),
            ]
        ).astype(int)

        # make a new square
        t, b, l, r = (
            min(corners[:, 0]) + top,
            max(corners[:, 0]) + top,
            min(corners[:, 1]) + left,
            max(corners[:, 1]) + left,
        )

        original_blocks.append([t, b, l, r])

    return original_blocks


def get_words_from_OCR(path: str, precition: int, rotated: bool = False, expansion: float = 1 / 6, simple: bool = False) -> Tuple[list, list, list, list]:
    """Get the words from an image with OCR

    :param path: (str) Path to the image
    :param precition: (int) precition of the OCR
    :param rotated: (bool) True if the image is rotated
    :param expansion: (float) Expansion of the image
    :param simple: (bool) True if the image is simple
    :return: (tuple) with the image, stats, centroids and ocr_info
    """
    ocr_info = detect_document(path, expansion, rotated, simple)

    blocks_, width, height, font_size, min_sizes, ppal_rotation, top, left = ocr_info

    imagen, stats, centroids = compute_blocks(ocr_info, precition, simple)

    return imagen, stats, centroids, ocr_info


def compute_blocks(ocr_info: tuple, precition: int, simple: bool) -> Tuple[list, list, list]:
    """Takes the detect_docuent result and compute it into connected components info

    :param ocr_info: (tuple) All that detect_document returns
    :param precition: (int) precition of the OCR
    :param simple: (bool) True if the image is simple
    :return: (tuple) with the image, stats and centroids
    """

    blocks_, width, height, font_size, min_sizes, ppal_rotation, top, left = ocr_info

    if min_sizes < precition:
        blocks = blocks_.copy()
        width_ = int(width) + 1
        height_ = int(height) + 1
        font_size_ = int(font_size)

    else:
        min_sizes = min_sizes / precition
        blocks = [
            [
                int(b[0] / min_sizes),
                int(b[1] / min_sizes),
                int(b[2] / min_sizes),
                int(b[3] / min_sizes),
                b[4],
            ]
            for b in blocks_
        ]

        width_ = int(width / min_sizes) + 1
        height_ = int(height / min_sizes) + 1
        font_size_ = int(font_size / min_sizes)

    imagen = -np.ones((width_, height_))
    stats = []
    centroids = []

    if not simple:
        i = 0
        while i in range(len(blocks)):

            ignore = False

            t, b, l, r, o = blocks[i]
            t_, b_, l_, r_, o_ = blocks_[i]

            # interlineado = int((b-t)/7)
            interlineado = 1
            words = set(imagen[t - interlineado : b + interlineado, l:r].flatten())
            # words = set(imagen[t:b,l:r].flatten())

            while words != {-1}:
                if len(words) == 0:
                    blocks.pop(i)
                    blocks_.pop(i)
                    ignore = True
                    break
                j = int(list(words - {-1})[0])
                tj, bj, lj, rj, oj = blocks[j]
                tj_, bj_, lj_, rj_, oj_ = blocks_[j]
                blocks[i] = min(t, tj), max(b, bj), min(l, lj), max(r, rj), oj + o
                blocks_[i] = (
                    min(t_, tj_),
                    max(b_, bj_),
                    min(l_, lj_),
                    max(r_, rj_),
                    oj_ + o_,
                )

                st = stats[j]
                imagen[st[1] : st[1] + st[3], st[0] : st[0] + st[2]] = -1

                for st in stats[j + 1 : i]:
                    imagen[st[1] : st[1] + st[3], st[0] : st[0] + st[2]] = (
                        imagen[st[1] : st[1] + st[3], st[0] : st[0] + st[2]] - 1
                    )

                blocks.pop(j)
                blocks_.pop(j)
                stats.pop(j)
                centroids.pop(j)
                # words_txt.pop(j)
                i = i - 1

                t, b, l, r, o = blocks[i]
                # interlineado = int((b-t)/7)
                # interlineado = 1
                words = set(imagen[t - interlineado : b + interlineado, l:r].flatten())
                # words = set(imagen[t:b,l:r].flatten())

            if not ignore:
                t, b, l, r = int(t), int(b), int(l), int(r)
                stats.append([l, t, r - l, b - t, (b - t) * (r - l)])
                centroids.append([(l + r) / 2, (t + b) / 2])
                # words_txt.append(o)
                imagen[t:b, l:r] = i

                i += 1

    else:
        for i in range(len(blocks)):

            t, b, l, r, o = blocks[i]
            t, b, l, r = int(t), int(b), int(l), int(r)
            stats.append([l, t, r - l, b - t, (b - t) * (r - l)])
            centroids.append([(l + r) / 2, (t + b) / 2])
            # words_txt.append(o)
            imagen[t:b, l:r] = i

            i += 1

    return imagen.astype(int), np.array(stats), np.array(centroids)


def ocr_block_2_word(block: tuple) -> Tuple[int, int, int, int, str]:
    """Takes a block and returns the word

    :param block: (tuple) with the block info
    :return: (tuple) with the word info
    """

    t1, l1, t2, r1, b1, l2, b2, r2, ocr = block

    t = min(t1, t2)
    l = max(l1, l2)
    b = min(b1, b2)
    r = max(r1, r2)

    return t, b, l, r, ocr


"""


# words features
def lbp_hist(imagen):
    n_points = 4
    radius = 3
    lbp = local_binary_pattern(imagen, n_points, radius, method = "default")
    lbp = np.histogram(lbp.ravel(), bins=np.arange(0, 2**n_points))[0]
    lbp[-1] = (imagen.shape[0]*imagen.shape[1])-lbp[-1]
    lbp = 2*lbp[:-1]/(1-lbp[-1])
    
    # Ahora verticales y horizontales
    return lbp


def rgb_hist(imagen):   
    im = ((imagen/32+1).astype(int)-1).astype(np.float32)
    histRGB = [0]*(3*8)
    for i in range(3):
        hist = cv2.calcHist([im],[i],None,[8],[0,8])
        hist[-1] = (im.size)-hist[-1]
        hist = hist/hist[-1]
        histRGB[i*8:(i+1)*8] = hist
        
    blue = np.sum(im[:,:,2].flatten())/im.size
    green = np.sum(im[:,:,1].flatten())/im.size
    red = np.sum(im[:,:,0].flatten())/im.size
    
    return [blue,green,red] + histRGB


def block_vision_features(imagen):
    
    rgb = rgb_hist(imagen)
    
    gray_image = cv2.cvtColor(imagen, cv2.COLOR_BGR2GRAY)
    
    lbp = lbp_hist(gray_image)
    horiz = cv2.resize(gray_image, (16,1), interpolation = cv2.INTER_LINEAR)[0,:]
    
    return list(rgb) + list(lbp) + list(horiz)


def ocr2visionfeatures(imagen_path, ocr_path = None):
    
    if type(imagen_path) is str:
        imagen = cv2.imread(imagen_path)
        if ocr_path is None:
            ocr_blocks = pandas.DataFrame(detect_document(imagen_path)[0])
            
        elif type(ocr_path) is str: ocr_blocks = pandas.read_csv(ocr_path)
        else: ocr_blocks = ocr_path
            
            
    else:
        imagen = imagen_path
        if ocr_path is None:
            cv2.imwrite('temporal_img.jpg', imagen)
            ocr_blocks = pandas.DataFrame(detect_document('temporal_img.jpg')[0])
            
        elif type(ocr_path) is str: ocr_blocks = pandas.read_csv(ocr_path)
        else: ocr_blocks = ocr_path
    
    blocks = np.array(ocr_blocks.copy())
    
    vision_features = []
    
    for i in range(len(blocks)):
        
        if len(blocks[i]) == 5: t, b, l, r, o = blocks[i]
        else: t, b, l, r, o = ocr_block_2_word(blocks[i])
            
        im = imagen[t:b,l:r,:]
        features = [ t, b, l, r, b-t, r-l, (b-t)/(r-l), (b-t)*(r-l), 1, 0] + list(np.array(block_vision_features(im)).astype(np.float32)) + [o]
        vision_features.append(features)
        
    heads = list(['top', 'botton', 'left', 'right',  'height', 'length', 'aspect_ratio', 'size', 'rows', 'max_cols', 'blue', 'red', 'green']
    + ['histRBG_' + str(i) for i in range(24)] + ['lbp_' + str(i) for i in range(14)]
    + ['horiz_' + str(i) for i in range(16)] + ['OCR'])

    features_dataframe = pandas.DataFrame(vision_features, columns = heads)
        
    return features_dataframe

"""
"""

# CASE 1

imagen = cv2.imread('invoice.jpg') # read the image as 3 chanel matrix (or have it in memory)

ocr_blocks = pandas.read_csv('invoiceocr.txt') # read the csv as a dataframe (or have it in memory)
#each block is [left_top_X, left_top_Y, right_top_X, right_top_Y, left_botton_X, left_botton_Y, right_botton_X, right_botton_Y, ocr_text])

ocr_blocks = np.array(ocr_blocks) # set it as np.array (not necesary)

vision_features = ocr2visionfeatures(imagen, ocr_blocks)
# get a numpy array with vision features and the ocr text





# CASE 2

# using only the paths (needs the image and the dataframe saved on disk)

imagen_path = 'invoice.jpg'
ocr_path = 'invoiceocr.txt' # dataframe saved as csv

vision_features = ocr2visionfeatures(imagen_path, ocr_path)



# CASE 3

# using only the image path and calling google vision api

imagen_path = 'invoice.jpg'
ocr_blocks = detect_document(image_path)

vision_features = ocr2visionfeatures(imagen_path, ocr_blocks)



# CASE 4

# using only the image path (calls google vision api)

imagen_path = 'invoice.jpg'
vision_features = ocr2visionfeatures(imagen_path)

"""


def box_vision_features(interest_box: list, words_features: list) -> list:
    """[summary]

    :param interest_box: [description]
    :param words_features: [description]
    :return: [description]
    """

    # eSTO HAY QUE HACERLO AUN

    [t, b, l, r] = interest_box.p
    words = interest_box.w
    # print(words_features.iloc[list(words)])
    words_features = np.array(words_features.iloc[list(words)])
    o = box.t
    if o == "":
        o = "".join(np.array(words_txt)[list(words_features[:, -1])])

    features = [t, b, l, r, b - t, r - l, (b - t) / (r - l), (b - t) * (r - l)]

    count_rows = []
    for feat in features:
        count_rows.append([feat[0], "i"])
        count_rows.append([feat[1], "0"])

    count_rows = np.array(count_rows)

    features = reatures + list(np.mean(words_features[7:7+38], axis=1))

    return features
