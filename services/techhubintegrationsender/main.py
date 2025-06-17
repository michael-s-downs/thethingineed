### This code is property of the GGAO ###


# Native imports
import os
import sys
from copy import deepcopy

# Custom imports
sys.path.append(os.getenv('LOCAL_COMMON_PATH'))
from integration_base import *


def process():
    requests_manager.restore_requests()

    logger.info(f"---- Listening from queue '{provider_resources.queue_url}'")
    while not killer.kill_now:
        # Poll a time for new messages, empty list if no new messages
        messages, messages_metadata = provider_resources.queue_read_messages(delete=provider_resources.queue_delete_on_read)

        # Load or update requests
        for message in messages:
            message_type = message.get('type', "")

            # Input request directly via queue instead of API
            if message_type not in ["request", "response"]:
                try:
                    logger.debug(f"Trying to parse message with unknown type '{message_type}'")
                    message, _ = receive_request(message)

                    if message.get('status', "") == "processing":
                        message_type = "request"
                    else:
                        raise Exception()
                except:
                    logger.warning(f"Unable to parse message with unknown type '{message_type}'")

            if message_type == "request":
                logger.info(f"---- Request received '{message.get('integration_id', '')}' ({message.get('status', '').upper()})")
                requests_manager.persist_request(message)
            elif message_type == "response":
                logger.info(f"---- Response received '{message.get('pid', '')}' ({message.get('status', '').upper()})")
                requests_manager.update_request(message)
            else:
                logger.warning(f"Ignoring message with unknown type '{message_type}'")

        if not provider_resources.queue_delete_on_read:
            # Quit from queue loaded messages
            provider_resources.queue_delete_message(messages_metadata)

        # Update status before process
        requests_manager.check_timeout(core_calls.request_internal_timeout)

        # Process new and pending requests (use a copy to conserve size)
        for integration_id, request_json in deepcopy(requests_manager.current_requests).items():
            process_request(request_json)

            # Clean from memory if not persisted in storage
            if not requests_manager.storage_persist_request:
                requests_manager.current_requests.pop(integration_id, {})

    # If loop breaks
    logger.info("---- Stopping service...")


if __name__ == '__main__':
    logger.info(f"---- Loading custom files ({os.getenv('INTEGRATION_NAME').upper()})")
    load_custom_files()

    logger.info(f"---- Launching service ({'NON ' if not requests_manager.storage_delete_request else ''}DELETE MODE)")
    process()
