from loguru import logger
from file_box.service import FileBoxServiceProtocol, ItemDTO, get_file_by_id


def test_upload_image_webp(get_file_service: FileBoxServiceProtocol) -> None:
    file_service = get_file_service
    file = open("./local/3.webp", "rb").read()
    item = ItemDTO(
        file_type="image",
        file_bytes=file,
    )
    file_response = file_service.upload_file(item)
    file_from_db = get_file_by_id(file_response.file_id)
    assert file_from_db is not None
    assert file_response.file_id == file_from_db.file_id
    

def test_image_have_compress_webp(get_file_service: FileBoxServiceProtocol) -> None:
    file_service = get_file_service
    file = open("./local/3.webp", "rb").read()
    item = ItemDTO(
        file_type="image",
        file_bytes=file,
    )
    file_response = file_service.upload_file(item)
    assert file_response.compress_info
    
    
def test_upload_image_jpeg(get_file_service: FileBoxServiceProtocol) -> None:
    file_service = get_file_service
    file = open("./local/test.jpeg", "rb").read()
    item = ItemDTO(
        file_type="image",
        file_bytes=file,
    )
    file_response = file_service.upload_file(item)
    file_from_db = get_file_by_id(file_response.file_id)
    assert file_from_db is not None
    assert file_response.file_id == file_from_db.file_id
    

def test_image_have_compress_jpeg(get_file_service: FileBoxServiceProtocol) -> None:
    file_service = get_file_service
    file = open("./local/test.jpeg", "rb").read()
    item = ItemDTO(
        file_type="image",
        file_bytes=file,
    )
    file_response = file_service.upload_file(item)
    assert file_response.compress_info
    
    
def test_upload_pdf(get_file_service: FileBoxServiceProtocol) -> None:
    file_service = get_file_service
    file = open("./local/Lorem_ipsum.pdf", "rb").read()
    item = ItemDTO(
        file_type="document",
        file_bytes=file,
    )
    file_response = file_service.upload_file(item)
    file_from_db = get_file_by_id(file_response.file_id)
    assert file_from_db is not None
    assert file_response.file_id == file_from_db.file_id
    

def test_get_image_by_id(get_file_service: FileBoxServiceProtocol) -> None:
    file_service = get_file_service
    file = open("./local/3.webp", "rb").read()
    item = ItemDTO(
        file_type="image",
        file_bytes=file,
    )
    file_response = file_service.upload_file(item)
    file_from_db = file_service.get_file_response(file_response.file_id)
    assert file_from_db is not None
    assert file_response.file_id == file_from_db.file_id
    
def test_get_file_by_id(get_file_service: FileBoxServiceProtocol) -> None:
    file_service = get_file_service
    file = open("./local/Lorem_ipsum.pdf", "rb").read()
    item = ItemDTO(
        file_type="document",
        file_bytes=file,
    )
    file_response = file_service.upload_file(item)
    file_from_db = file_service.get_file_response(file_response.file_id)
    assert file_from_db is not None
    assert file_response.file_id == file_from_db.file_id
    
def test_file_have_meta_data(get_file_service: FileBoxServiceProtocol) -> None:
    file_service = get_file_service
    file = open("./local/3.webp", "rb").read()
    item = ItemDTO(
        file_type="image",
        file_bytes=file,
        meta_data={"test": "test"}
    )
    file_response = file_service.upload_file(item)
    assert file_response.meta_data == {"test": "test"}
    
    
    


    