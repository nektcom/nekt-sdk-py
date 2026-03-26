"""Volume service for managing volumes and file uploads."""

from __future__ import annotations

import logging
import mimetypes
import os
from typing import TYPE_CHECKING, Any

import requests

from nekt.exceptions import FileUploadError

if TYPE_CHECKING:
    from nekt.api import NektAPI

logger = logging.getLogger(__name__)


class VolumeService:
    """Service for volume operations including multipart file upload.

    Provides methods to load volume file listings, create new volumes,
    and save files using a 3-step multipart upload process.

    Usage::

        from nekt.api import NektAPI
        from nekt.services.volumes import VolumeService

        api = NektAPI(token)
        volumes = VolumeService(api)
        files = volumes.load_volume("layer", "volume")
    """

    def __init__(self, api: NektAPI) -> None:
        self._api = api

    def load_volume(self, layer_name: str, volume_name: str) -> list[dict[str, str]]:
        """Load volume file listings from the API.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.

        Returns:
            List of file path dictionaries.

        Raises:
            VolumeNotFoundError: If the volume doesn't exist.
        """
        return self._api.load_volume(layer_name, volume_name)

    def create_volume(
        self,
        layer_name: str,
        volume_name: str,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Create a new volume in a layer.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume to create.
            description: Optional description.

        Returns:
            Dict containing volume metadata (id, name, slug, s3_path, etc.).

        Raises:
            APIError: If the layer is not found or creation fails.
        """
        return self._api.create_volume(layer_name, volume_name, description)

    def save_file(
        self,
        layer_name: str,
        volume_name: str,
        file_path: str,
        file_name: str | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        """Save a file to a volume using multipart upload.

        Performs a 3-step upload process:
        1. Create volume file entry and get presigned upload URLs.
        2. Upload file in 100MB chunks to presigned URLs.
        3. Complete the multipart upload with ETags.

        Args:
            layer_name: Name of the layer.
            volume_name: Name of the volume.
            file_path: Local path to the file to upload.
            file_name: Optional name for the file in the volume
                (defaults to basename of file_path).
            description: Optional description for the file.

        Returns:
            Dict with file metadata: id, name, file_size, file_type, description.

        Raises:
            FileUploadError: If the file is not found, upload fails,
                or completion fails.
        """
        # Step 1: Validate file and compute metadata
        if not os.path.exists(file_path):
            raise FileUploadError(f"File not found: {file_path}")

        file_size = os.path.getsize(file_path)
        name = file_name or os.path.basename(file_path)
        file_type, _ = mimetypes.guess_type(file_path)
        file_type = file_type or "application/octet-stream"

        logger.info(
            "Saving file %s (%d bytes) to %s/%s",
            name, file_size, layer_name, volume_name,
        )

        # Step 2: Create volume file and get presigned URLs
        try:
            response = self._api.create_volume_file(
                layer_name=layer_name,
                volume_name=volume_name,
                name=name,
                file_size=file_size,
                file_type=file_type,
                description=description,
            )
        except Exception as e:
            raise FileUploadError(f"Failed to create volume file: {e}") from e

        file_id = response["id"]
        presigned_urls = response.get("presigned_url_list", [])

        if not presigned_urls:
            raise FileUploadError("No presigned URLs returned from API")

        # Step 3: Upload file in 100MB chunks via presigned URLs
        part_size = 100 * 1024 * 1024  # 100MB
        parts: list[dict[str, Any]] = []

        try:
            with open(file_path, "rb") as f:
                for url_info in presigned_urls:
                    part_number = url_info.get("part_number", len(parts) + 1)
                    presigned_url = url_info.get("presigned_url")

                    chunk = f.read(part_size)
                    if not chunk:
                        break

                    logger.debug(
                        "Uploading part %d (%d bytes)", part_number, len(chunk),
                    )

                    # Use requests.put directly -- presigned URLs are
                    # pre-authenticated cloud storage URLs, not Nekt API endpoints
                    upload_response = requests.put(presigned_url, data=chunk)
                    upload_response.raise_for_status()

                    etag = upload_response.headers.get("ETag", "").strip('"')
                    parts.append({"etag": etag, "part_number": part_number})

        except requests.RequestException as e:
            raise FileUploadError(f"Failed to upload file part: {e}") from e
        except OSError as e:
            raise FileUploadError(f"Failed to read file: {e}") from e

        # Step 4: Complete multipart upload
        try:
            self._api.complete_volume_file_upload(
                layer_name=layer_name,
                volume_name=volume_name,
                file_id=file_id,
                parts=parts,
            )
        except Exception as e:
            raise FileUploadError(f"Failed to complete upload: {e}") from e

        logger.info("File %s saved to %s/%s", name, layer_name, volume_name)

        return {
            "id": file_id,
            "name": name,
            "file_size": file_size,
            "file_type": file_type,
            "description": description,
        }
