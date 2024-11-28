### This code is property of the GGAO ###


import pytest
import os
from unittest.mock import patch, MagicMock, AsyncMock
from pcutils.persist import PersistManager, PersistDict, Conversation
from common.errors.genaierrors import PrintableGenaiError
from common.genai_controllers import storage_containers, db_dbs, set_storage, set_db, upload_object, delete_file

@pytest.fixture
def persist_manager():
    pm = PersistManager()
    pm.parse({"persist": {"type": "chat", "params": {}}})
    return pm

@pytest.fixture
def persist_dict():
    return PersistDict()

def test_persist_manager_init():
    pm = PersistManager()
    assert pm.type is None
    assert pm.params is None
    assert pm.defaults_dict["max_persistence"] == 10

@patch("pcutils.persist.PersistDict")
def test_persist_manager_parse(mock_persist_dict, persist_manager):
    compose_config = {
        "persist": {
            "type": "chat",
            "params": {}
        }
    }
    result = persist_manager.parse(compose_config)
    assert result == persist_manager
    assert persist_manager.type == "chat"
    assert persist_manager.params == {}

def test_persist_manager_parse_no_config(persist_manager):
    result = persist_manager.parse({})
    assert result is None

def test_persist_manager_get_param(persist_manager):
    params = {"type": "chat", "params": {}}
    result = persist_manager.get_param(params, "type", str)
    assert result == "chat"

def test_persist_manager_run(persist_manager):
    template = [
        {
            'action': 'llm_action',
            'action_params': {
                'params': {
                    "platform_metadata": {
                        "platform": "azure"
                    },
                    'session_id': ''
                }
            }
        }
    ]
    session_id = "session_123"
    PD = PersistDict()
    reformulated = False

    result = persist_manager.run(template, session_id, PD, reformulated)
    assert result[0]['action_params']['params']['session_id'] == session_id

def test_persist_manager_run_no_chat_type(persist_manager):
    persist_manager.type = "not_chat"
    template = [{'action': 'llm_action', 'action_params': {'params': {}}}]
    result = persist_manager.run(template, "session_123", PersistDict(), False)
    assert result == template

def test_persist_dict_add_new_session(persist_dict):
    session_id = "session_123"
    persistence = {"data": "test"}
    persist_dict.PD.clear()
    persist_dict.add(persistence, session_id=session_id)

    assert session_id in persist_dict.PD
    assert len(persist_dict.PD[session_id]) == 1

def test_persist_dict_remove_last(persist_dict):
    session_id = "session_123"
    persistence1 = {"data": "test1"}
    
    persist_dict.PD.clear()
    persist_dict.add(persistence1, session_id=session_id)
    persist_dict.add(persistence1, session_id=session_id)
    persist_dict.add(persistence1, session_id=session_id)
    persist_dict.remove_last(session_id)

    assert len(persist_dict.PD[session_id]) == 2

def test_persist_dict_update_last(persist_dict):
    session_id = "session_123"
    persistence1 = {"data": "test1"}
    persistence2 = {"data": "test2"}
    
    persist_dict.PD.clear()
    persist_dict.add(persistence1, session_id=session_id)
    persist_dict.add(persistence1, session_id=session_id)
    persist_dict.update_last(persistence=persistence2, session_id = session_id)

    assert len(persist_dict.PD[session_id]) == 2
    assert persist_dict.PD[session_id][-1] == persistence2

def test_persist_dict_update_last_not_dict(persist_dict):
    session_id = "session_123"
    persistence1 = []
    persist_dict.PD.clear()
    
    persist_dict.add({}, session_id=session_id)
    conv = persist_dict.get_conversation(session_id)
    with pytest.raises(PrintableGenaiError) as exc_info:
        conv.update_last(persistence1)
    assert "must be a dict" in str(exc_info.value)

def test_persist_dict_add_existing_session(persist_dict):
    session_id = "session_123"
    persistence1 = {"data": "test1"}
    persistence2 = {"data": "test2"}
    
    persist_dict.PD.clear()
    persist_dict.add(persistence1, session_id=session_id)
    persist_dict.add(persistence2, session_id=session_id, max_persistence=4)

    assert len(persist_dict.PD[session_id]) == 2
    assert persist_dict.PD[session_id].max_persistence == 4

def test_persist_dict_add_not_dict(persist_dict):
    session_id = "session_123"
    persistence1 = []
    
    persist_dict.PD.clear()

    persist_dict.add(persistence1, session_id=session_id)
    with pytest.raises(PrintableGenaiError) as exc_info:
        persist_dict.add(persistence1, session_id=session_id)
    assert "must be a dict" in str(exc_info.value)

def test_persist_dict_get_conversation(persist_dict):
    session_id = "session_123"
    persistence = {"data": "test"}
    persist_dict.PD.clear()
    persist_dict.add(persistence, session_id=session_id)

    conv = persist_dict.get_conversation(session_id)
    assert isinstance(conv, Conversation)
    assert len(conv) == 1

def test_persist_dict_get_from_redis_raises_error(persist_dict):
    session_id = "session_12345"
    tenant = "tenant_abc"

    with patch('pcutils.persist.get_value', side_effect=Exception("Redis error")):
        with pytest.raises(PrintableGenaiError, match="Error getting session from redis"):
            persist_dict.get_from_redis(session_id, tenant)

def test_persist_dict_save_to_redis(persist_dict):
    set_db(db_dbs)
    session_id = "session_123"
    tenant = "techhubragmeal"
    persist_dict.PD.clear()
    persist_dict.add({
            "user": "Propositos de año viejo?",
            "assistant": "Claro, aquí tienes algunas ideas",
            "n_tokens": 16,
            "input_tokens": 29,
            "output_tokens": 273
        }, session_id=session_id)

    with patch('pcutils.persist.update_status', return_value=None) as mock_update_status:
        persist_dict.save_to_redis(session_id, tenant)
    mock_update_status.assert_called_once()
    assert persist_dict.get_conversation(session_id) == []


def test_conversation_add(persist_dict):
    persistence = {"data": "test"}
    conv = Conversation([], max_persistence=3)

    conv.add(persistence)
    assert len(conv) == 1

    conv.add({"data": "test2"})
    conv.add({"data": "test3"})
    conv.add({"data": "test4"})  # Should trigger removal
    assert len(conv) == 3  # Only 3 should remain

def test_conversation_update_last(persist_dict):
    persistence = {"data": "test"}
    conv = Conversation([persistence], max_persistence=3)

    conv.update_last({"data": "updated_test"})
    assert conv[-1] == {"data": "updated_test"}

def test_conversation_remove_last(persist_dict):
    persistence1 = {"data": "test1"}
    persistence2 = {"data": "test2"}
    conv = Conversation([persistence1, persistence2], max_persistence=3)

    conv.remove_last()
    assert len(conv) == 1
    assert conv[-1] == persistence1

def test_conversation_is_response(persist_dict):
    conv = Conversation([{"assistant": "response"}], max_persistence=3)

    assert conv.is_response() is True

def test_conversation_is_not_response(persist_dict):
    conv = Conversation([{"user": "query"}], max_persistence=3)

    assert conv.is_response() is False

def test_persist_manager_parse_no_persist(persist_manager):
    # Test when persist key does not exist
    compose_config = {}
    pm = PersistManager()
    result = pm.parse(compose_config)
    assert result is None
    assert pm.type is None
    assert pm.params is None

def test_persist_manager_get_param_key_does_not_exist(persist_manager):
    params = {"type": "chat"}
    with pytest.raises(PrintableGenaiError) as exc_info:
        persist_manager.get_param(params, "non_existent_key", str)
    assert "Default param" in str(exc_info.value)


def test_persist_dict_getitem_invalid_key_type(persist_dict):
    # Test with invalid session_id type (non-string value)
    with pytest.raises(PrintableGenaiError) as exc_info:
        persist_dict[123]  # Non-string session_id
    assert "Session id must be a string" in str(exc_info.value)

def test_persist_dict_getitem(persist_dict):
    # Test with invalid session_id type (non-string value)
    session_id = "session_123"
    persist_dict.add({"data": "test"}, session_id=session_id)
    assert persist_dict[session_id] == [{"data": "test"}]

def test_persist_dict_update_context(persist_dict):
    session_id = "session_123"
    context = "new_context"
    persist_dict.PD.clear()
    persist_dict.add({"data": "test"}, session_id=session_id)

    persist_dict.update_context(session_id, context)
    assert persist_dict.PD[session_id].context == context

def test_conversation_get_n_last_full_conversation(persist_dict):
    conv = Conversation([{"data": "test1"}, {"data": "test2"}], max_persistence=3)
    result = conv.get_n_last(5)  # Requesting more than available
    assert len(result) == 2  # Returns all entries since n > len(conv)

def test_conversation_get_n_last_partial_conversation(persist_dict):
    conv = Conversation([{"data": "test1"}, {"data": "test2"}, {"data": "test3"}], max_persistence=3)
    result = conv.get_n_last(2)  # Requesting the last 2 entries
    assert result == [{"data": "test2"}, {"data": "test3"}]

def test_conversation_add_exceeding_max_persistence(persist_dict):
    # Test that exceeding max_persistence removes oldest entry
    conv = Conversation([{"data": "test1"}], max_persistence=3)
    conv.add({"data": "test2"})
    conv.add({"data": "test3"})
    conv.add({"data": "test4"})  # Should remove "test1"
    assert len(conv) == 3
    assert conv[0] == {"data": "test2"}

def test_get_session_from_redis(persist_dict):
    session_id = "session_123"
    tenant = "techhubragemeal"

    persist_dict.PD.clear()
    persist_dict.get_from_redis(session_id, tenant)
    assert len(persist_dict.PD[session_id]) == 1

def test_error_saveing_session_to_redis(persist_dict):
    session_id = "session_123_kk"
    tenant = "techhubragemeal"

    persist_dict.add({
            "user": "Propositos de año viejo?",
            "assistant": "Claro, aquí tienes algunas ideas",
            "n_tokens": 16,
            "input_tokens": 29,
            "output_tokens": 273
        }, session_id=session_id)
    persist_dict.REDIS_ORIGIN = None

    with patch('pcutils.persist.update_status', side_effect=Exception("Error saving to redis")):
        with pytest.raises(Exception, match="Error saving session to redis"):
            persist_dict.save_to_redis(session_id, tenant)


def test_error_saving_session_to_redis_conv_not_response(persist_dict):
    session_id = "session_123_kk"
    tenant = "techhubragemeal"

    persist_dict.add({}, session_id=session_id)
    persist_dict.REDIS_ORIGIN = None

    with pytest.raises(PrintableGenaiError, match="500"):
        persist_dict.save_to_redis(session_id, tenant)