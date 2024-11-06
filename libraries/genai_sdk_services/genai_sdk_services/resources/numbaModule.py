### This code is property of the GGAO ###


# Installed import
import numpy as np
from numba import jit

LONG_HIST_GRAY = 256
LONG_HIST_COLOR = 16
LONG_HIST_GRAYRGB = LONG_HIST_GRAY + 3 * LONG_HIST_COLOR
NUM_FEAT_RC = 300


@jit
def simpleblur(im: bytes, horiz: float, vert: float):
    """Performs a simple blur to the grayscale image. It is basically a convolution woth a ones(horiz,vert kernel

    :param im: Input image
    :param horiz: Horizontal size of the kernel
    :param vert: Vertical size of the kernel
    :return: Blurred image
    """
    m, n = im.shape
    blurred = np.zeros((m, n))

    hcut, vcut = int((horiz - 1) / 2), int((vert - 1) / 2)
    kersize = float(vert * horiz)

    amp = np.zeros(((im.shape[0] + vert - 1), im.shape[1] + horiz - 1))

    amp[vcut : amp.shape[0] - vcut, hcut : amp.shape[1] - hcut] = im
    amp[:vcut, hcut:-hcut] = im[::-1, :][-vcut:, :]
    amp[-vcut:, hcut:-hcut] = im[::-1, :][:vcut, :]
    amp[vcut:-vcut:, :hcut] = im[:, ::-1][:, -hcut:]
    amp[vcut:-vcut:, -hcut:] = im[:, ::-1][:, :hcut]
    amp[:vcut, :hcut] = im[::-1, ::-1][-vcut:, -hcut:]
    amp[:vcut, -hcut:] = im[::-1, ::-1][-vcut:, :hcut]
    amp[-vcut:, :hcut] = im[::-1, ::-1][:vcut, -hcut:]
    amp[-vcut:, -hcut:] = im[::-1, ::-1][:vcut, :hcut]

    for i in range(m):
        for j in range(n):
            blurred[i, j] = np.sum(amp[i : i + vert, j : j + horiz]) / kersize

    return blurred


@jit
def more_pag_histogram(np_image: list) -> list:
    """Gets histograms from pages different than the first

    :param np_image: (list) Input image
    :return: (list) Histograms
    """

    # Number of feats for the rows and cols histograms
    m, n, c = np_image.shape
    # Create a gray image with padding
    gray = np.zeros((m + 2, n + 2))
    # For the gray, r, g and b histograms
    hist_gray = np.zeros(LONG_HIST_GRAY)
    hist_r = np.zeros(LONG_HIST_COLOR)
    hist_g = np.zeros(LONG_HIST_COLOR)
    hist_b = np.zeros(LONG_HIST_COLOR)

    # To store all the histograms
    hist_total = np.zeros(LONG_HIST_GRAYRGB)

    DIVISOR = 256 / LONG_HIST_COLOR

    for i in range(m):
        for j in range(n):
            # For every pixel we get the value of each channel and gray
            R = np_image[i, j, 2]
            G = np_image[i, j, 1]
            B = np_image[i, j, 0]
            valor_gris = 0.07 * B + 0.72 * G + 0.21 * R
            gray[i + 1, j + 1] = valor_gris

            # Compute the GRGB histogram
            hist_r[int(R / DIVISOR)] += 1
            hist_g[int(G / DIVISOR)] += 1
            hist_b[int(B / DIVISOR)] += 1
            hist_gray[int(valor_gris)] += 1

    # Concatenate all the info from the histograms
    hist_total[0:LONG_HIST_GRAY] = hist_gray
    hist_total[LONG_HIST_GRAY : LONG_HIST_GRAY + LONG_HIST_COLOR] = hist_r
    hist_total[
        LONG_HIST_GRAY + LONG_HIST_COLOR : LONG_HIST_GRAY + 2 * LONG_HIST_COLOR
    ] = hist_g
    hist_total[LONG_HIST_GRAY + 2 * LONG_HIST_COLOR : LONG_HIST_GRAYRGB] = hist_b

    return hist_total


@jit
def gray_image(image: list) -> list:
    """Converts image into grayscale

    :param image: (list) Input image (in colour)
    :return: (list) Grayscale image
    """
    m, n, c = image.shape
    gray = np.zeros((m, n))
    image.astype(np.float32)
    for i in range(m):
        for j in range(n):
            # For every pixel we get the value of each channel and gray
            gray[i, j] = (image[i, j, 0] + image[i, j, 1] + image[i, j, 2]) / 3.0

    return gray


@jit
def features_histogram(np_image: list) -> list:
    """Gets all the concatenated histograms (LBP,RGB,cols and rows) used as features from an image

    :param np_image: (list) Input image
    :return: (list) Histograms
    """
    # Number of feats for the rows and cols histograms
    m, n, c = np_image.shape
    # Create a gray image with padding
    gray = np.zeros((m + 2, n + 2))
    # For the gray, r, g and b histograms
    hist_gray = np.zeros(LONG_HIST_GRAY)
    hist_r = np.zeros(LONG_HIST_COLOR)
    hist_g = np.zeros(LONG_HIST_COLOR)
    hist_b = np.zeros(LONG_HIST_COLOR)
    # To merge all the GRGB histograms (could be done directly...)
    hist_grbg = np.zeros(LONG_HIST_GRAYRGB)
    # For the rows and cols histogram
    hist_rows = np.zeros(m)
    hist_cols = np.zeros(n)
    # For the LBP histogram
    hist_lbp = np.zeros(256)
    # To store all the histograms
    hist_total = np.zeros(LONG_HIST_GRAYRGB + 255 + 2 * NUM_FEAT_RC + 1)
    DIVISOR = 256 / LONG_HIST_COLOR
    for i in range(m):
        for j in range(n):
            # For every pixel we get the value of each channel and gray
            R = np_image[i, j, 2]
            G = np_image[i, j, 1]
            B = np_image[i, j, 0]
            valor_gris = 0.07 * B + 0.72 * G + 0.21 * R
            gray[i + 1, j + 1] = valor_gris

            # Compute the GRGB histogram
            hist_r[int(R / DIVISOR)] += 1
            hist_g[int(G / DIVISOR)] += 1
            hist_b[int(B / DIVISOR)] += 1
            hist_gray[int(valor_gris)] += 1

            # Compute the rows and cols histogram
            hist_rows[i] += valor_gris
            hist_cols[j] += valor_gris

    # Compute the LBP histogram from the padded gray image
    for i in range(m):
        for j in range(n):
            val = gray[i + 1, j + 1]
            lbp = (
                128 * (gray[i, j] >= val)
                + 64 * (gray[i, j + 1] >= val)
                + 32 * (gray[i, j + 2] >= val)
                + 16 * (gray[i + 1, j + 2] >= val)
                + 8 * (gray[i + 2, j + 2] >= val)
                + 4 * (gray[i + 2, j + 1] >= val)
                + 2 * (gray[i + 2, j] >= val)
                + (gray[i + 1, j] >= val)
            )
            hist_lbp[lbp] += 1

    # Invert the cols and rows histograms
    hist_cols = (np.max(hist_cols) - hist_cols) / 255
    hist_rows = (np.max(hist_rows) - hist_rows) / 255

    # conversion of the size of the rows and cols histograms
    feats_rows = np.zeros(NUM_FEAT_RC)
    num_puntos = m
    paso = num_puntos / NUM_FEAT_RC
    inicio = 0
    for i in range(NUM_FEAT_RC):
        final = inicio + paso
        inicio_int = int(np.floor(inicio))
        final_int = int(np.floor(final))
        feats_rows[i] = np.max(hist_rows[inicio_int:final_int])

        inicio = final
    feats_cols = np.zeros(NUM_FEAT_RC)
    num_puntos = n
    paso = num_puntos / NUM_FEAT_RC
    inicio = 0
    for i in range(NUM_FEAT_RC):
        final = inicio + paso
        inicio_int = int(np.floor(inicio))
        final_int = int(np.floor(final))
        feats_cols[i] = np.max(hist_cols[inicio_int:final_int])

        inicio = final

    # Concatenate all the info from the histograms
    hist_total[0:LONG_HIST_GRAY] = hist_gray
    hist_total[LONG_HIST_GRAY : LONG_HIST_GRAY + LONG_HIST_COLOR] = hist_r
    hist_total[
        LONG_HIST_GRAY + LONG_HIST_COLOR : LONG_HIST_GRAY + 2 * LONG_HIST_COLOR
    ] = hist_g
    hist_total[LONG_HIST_GRAY + 2 * LONG_HIST_COLOR : LONG_HIST_GRAYRGB] = hist_b

    hist_total[LONG_HIST_GRAYRGB : LONG_HIST_GRAYRGB + 255] = np.log10(
        hist_lbp[:-1] + 1
    )

    hist_total[-2 * NUM_FEAT_RC - 1 : -NUM_FEAT_RC - 1] = feats_rows
    hist_total[-NUM_FEAT_RC - 1 : -1] = feats_cols
    hist_total[-1] = m / n

    return hist_total

