### This code is property of the GGAO ###


# Native imports
import os, platform, json

# Installed imports
import pytest
from unittest.mock import MagicMock, patch
from unittest import mock

# Local imports
from common.storage_manager import ManagerStorage, LLMStorageManager, IRStorageManager, BaseStorageManager
from common.errors.genaierrors import PrintableGenaiError

class TestStorageManager:
    conf = {'type': "IRStorage", 'workspace': "test", 'origin': "test"}

    def test_get_possible_managers(self):
        platforms = ManagerStorage.get_possible_platforms()
        assert platforms == ["IRStorage", "LLMStorage"]

    def test_all_managers(self):
        self.conf['type'] = "LLMStorage"
        llm_storage = ManagerStorage.get_file_storage(self.conf)
        self.conf['type'] = "IRStorage"
        ir_storage = ManagerStorage.get_file_storage(self.conf)

        assert isinstance(llm_storage, LLMStorageManager)
        assert isinstance(ir_storage, IRStorageManager)

    def test_wrong_manager(self):
        self.conf['type'] = "nonexistent"
        with pytest.raises(PrintableGenaiError):
            ManagerStorage.get_file_storage(self.conf)

class TestBaseStorageManager:

    def load_file(self):
        # Wrong case
        storage_manager = BaseStorageManager("test", "test")
        assert storage_manager.load_file("test", "test.json") is None

        # Properly loaded
        with patch('common.genai_controllers.load_file') as mock_load_file:
            mock_load_file.return_value = json.dumps({"test": "test"}).encode('utf-8')
            assert len(storage_manager.load_file("test", "test.json")) > 0
class TestIRStorageManager:
    execution_path = os.path.abspath(os.getcwd())
    separator = "\\" if platform.system() == "Windows" else "/"
    default_models_path = execution_path + separator + "test" + separator + "src" + separator + "ir" + separator + "conf" + separator + "default_embedding_models.json"
    models_config_path = execution_path + separator + "test" + separator + "src" + separator + "ir" + separator + "conf" + separator + "models_config.json"

    def test_embedding_equivalences(self):
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:

            with open(self.default_models_path) as f:
                mock_load_file.return_value = f.read()

            default_equivalences = IRStorageManager("test", "test").get_embedding_equivalences()
            assert default_equivalences['bm25'] == "bm25"

    def test_available_embedding_models(self):
        # File not found
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            mock_load_file.return_value = None
            with pytest.raises(PrintableGenaiError):
                IRStorageManager("test", "test").get_available_embedding_models()

        # Embeddings key not found (wrong file configuration)
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            mock_load_file.return_value = json.dumps({"wrong_key": {}})
            with pytest.raises(PrintableGenaiError):
                IRStorageManager("test", "test").get_available_embedding_models()

        # Retrieval mode
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            with open(self.models_config_path) as f:
                mock_load_file.return_value = f.read()
            available_models = IRStorageManager("test", "test").get_available_embedding_models(inforetrieval_mode=True)
            assert len(available_models) > 0 and isinstance(available_models, list)

            available_models = IRStorageManager("test", "test").get_available_embedding_models(inforetrieval_mode=False)
            assert len(available_models) > 0 and isinstance(available_models, dict)

    def test_unique_embedding_models(self):
        # File not found
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            mock_load_file.return_value = None
            with pytest.raises(PrintableGenaiError):
                IRStorageManager("test", "test").get_unique_embedding_models()

        # Properly loaded
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            with open(self.models_config_path) as f:
                mock_load_file.return_value = f.read()
            unique_embedding_models = IRStorageManager("test", "test").get_unique_embedding_models()
            assert len(unique_embedding_models) > 0 and isinstance(unique_embedding_models, set)

    def test_get_available_pools(self):
        # File not found
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            mock_load_file.return_value = None
            with pytest.raises(PrintableGenaiError):
                IRStorageManager("test", "test").get_available_pools()

        # Not pools in models
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            with open(self.models_config_path) as f:
                embedding_models = json.loads(f.read())
                for platform, models in embedding_models.get('embeddings').items():
                    for model in models:
                        model['model_pool'] = []
                mock_load_file.return_value = json.dumps(embedding_models).encode('utf-8')
            with pytest.raises(PrintableGenaiError):
                IRStorageManager("test", "test").get_available_pools()

        # Properly loaded
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            with open(self.models_config_path) as f:
                mock_load_file.return_value = f.read()
            available_pools = IRStorageManager("test", "test").get_available_pools()
            assert len(available_pools) > 0 and isinstance(available_pools, dict)

    def test_get_pools_per_embedding_model(self):
        # File not found
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            mock_load_file.return_value = None
            with pytest.raises(PrintableGenaiError):
                IRStorageManager("test", "test").get_pools_per_embedding_model()

      # Not pools in models
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            with open(self.models_config_path) as f:
                embedding_models = json.loads(f.read())
                for platform, models in embedding_models.get('embeddings').items():
                    for model in models:
                        model['model_pool'] = []
                mock_load_file.return_value = json.dumps(embedding_models).encode('utf-8')
            with pytest.raises(PrintableGenaiError):
                IRStorageManager("test", "test").get_pools_per_embedding_model()

        # Properly loaded
        with patch('common.storage_manager.IRStorageManager.load_file') as mock_load_file:
            with open(self.models_config_path) as f:
                mock_load_file.return_value = f.read()
            available_pools = IRStorageManager("test", "test").get_pools_per_embedding_model()
            assert len(available_pools) > 0 and isinstance(available_pools, dict)


class TestLLMStorageManager:
    execution_path = os.path.abspath(os.getcwd())
    separator = "\\" if platform.system() == "Windows" else "/"
    models_config_path = execution_path + separator + "test" + separator + "src" + separator + "ir" + separator + "conf" + separator + "models_config.json"


    def test_get_available_pools(self):
        # File not found
        with patch('common.storage_manager.LLMStorageManager.load_file') as mock_load_file:
            mock_load_file.return_value = None
            with pytest.raises(PrintableGenaiError):
                LLMStorageManager("test", "test").get_available_pools()

        # Not pools in models
        with patch('common.storage_manager.LLMStorageManager.load_file') as mock_load_file:
            with open(self.models_config_path) as f:
                embedding_models = json.loads(f.read())
                for platform, models in embedding_models.get('LLMs').items():
                    for model in models:
                        model['model_pool'] = []
                mock_load_file.return_value = json.dumps(embedding_models).encode('utf-8')
            with pytest.raises(PrintableGenaiError):
                LLMStorageManager("test", "test").get_available_pools()

        # Properly loaded
        with patch('common.storage_manager.LLMStorageManager.load_file') as mock_load_file:
            with open(self.models_config_path) as f:
                mock_load_file.return_value = f.read()
            available_pools = LLMStorageManager("test", "test").get_available_pools()
            assert len(available_pools) > 0 and isinstance(available_pools, dict)

    def test_available_models(self):
        # File not found
        with patch('common.storage_manager.LLMStorageManager.load_file') as mock_load_file:
            mock_load_file.return_value = None
            with pytest.raises(PrintableGenaiError):
                LLMStorageManager("test", "test").get_available_models()

        # LLMs key not found (wrong file configuration)
        with patch('common.storage_manager.LLMStorageManager.load_file') as mock_load_file:
            mock_load_file.return_value = json.dumps({"wrong_key": {}})
            with pytest.raises(PrintableGenaiError):
                LLMStorageManager("test", "test").get_available_models()

        # Retrieval mode
        with patch('common.storage_manager.LLMStorageManager.load_file') as mock_load_file:
            with open(self.models_config_path) as f:
                mock_load_file.return_value = f.read()
            available_models = LLMStorageManager("test", "test").get_available_models()
            assert len(available_models) > 0 and isinstance(available_models, dict)
