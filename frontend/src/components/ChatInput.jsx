import { useState } from "react";
import "./ChatInput.css";

const ChatInput = ({ onSubmit, isLoading = false, placeholder = "Ask me anything..." }) => {
    const [selectedFiles, setSelectedFiles] = useState([]);
    const [uploadingFiles, setUploadingFiles] = useState(false);

    const handleFileSelect = (e) => {
        const files = Array.from(e.target.files);
        // Filter only image files
        const imageFiles = files.filter(file => file.type.startsWith('image/'));
        setSelectedFiles(prev => [...prev, ...imageFiles]);
    };

    const removeFile = (index) => {
        setSelectedFiles(prev => prev.filter((_, i) => i !== index));
    };

    const uploadImagesToBackend = async (files) => {
        const uploadPromises = files.map(async (file) => {
            const formData = new FormData();
            formData.append('file', file);

            try {
                const response = await fetch('http://localhost:8001/upload-image', {
                    method: 'POST',
                    body: formData,
                });

                if (!response.ok) {
                    throw new Error(`Failed to upload ${file.name}`);
                }

                const result = await response.json();
                return {
                    success: true,
                    fileName: file.name,
                    imageData: result.data
                };
            } catch (error) {
                console.error(`Error uploading ${file.name}:`, error);
                return {
                    success: false,
                    fileName: file.name,
                    error: error.message
                };
            }
        });

        return await Promise.all(uploadPromises);
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        const text = e.target.text.value;
        if (!text && selectedFiles.length === 0) return;

        let imageUrls = [];

        // Upload images to backend if any files are selected
        if (selectedFiles.length > 0) {
            setUploadingFiles(true);
            try {
                const uploadResults = await uploadImagesToBackend(selectedFiles);

                // Filter successful uploads and get URLs
                const successfulUploads = uploadResults.filter(result => result.success);
                imageUrls = successfulUploads.map(result => ({
                    url: result.imageData.url,
                    thumbnail_url: result.imageData.thumbnail_url,
                    file_id: result.imageData.file_id,
                    fileName: result.fileName,
                    width: result.imageData.width,
                    height: result.imageData.height
                }));

                // Check for failed uploads
                const failedUploads = uploadResults.filter(result => !result.success);
                if (failedUploads.length > 0) {
                    console.warn('Some files failed to upload:', failedUploads);
                    // You might want to show a toast notification here
                }
            } catch (error) {
                console.error('Error uploading images:', error);
                setUploadingFiles(false);
                return; // Don't submit if upload fails
            }
            setUploadingFiles(false);
        }

        // Prepare submission data
        const submissionData = {
            text: text,
            images: imageUrls
        };

        // Call the parent's onSubmit function
        if (onSubmit) {
            await onSubmit(submissionData);
        }

        // Reset form
        e.target.reset();
        setSelectedFiles([]);
    };

    return (
        <div className="chatInputContainer">
            <form onSubmit={handleSubmit}>
                {selectedFiles.length > 0 && (
                    <div className="filePreview">
                        {selectedFiles.map((file, index) => (
                            <div key={index} className="fileItem">
                                {file.type.startsWith('image/') && (
                                    <div className="imagePreview">
                                        <img
                                            src={URL.createObjectURL(file)}
                                            alt={file.name}
                                            className="previewImage"
                                        />
                                    </div>
                                )}
                                <div className="fileInfo">
                                    <span className="fileName">{file.name}</span>
                                    <span className="fileSize">({(file.size / 1024).toFixed(1)} KB)</span>
                                </div>
                                <button
                                    type="button"
                                    className="removeFile"
                                    onClick={() => removeFile(index)}
                                    disabled={uploadingFiles}
                                >
                                    ×
                                </button>
                            </div>
                        ))}
                        {uploadingFiles && (
                            <div className="uploadingIndicator">
                                <span>Uploading images...</span>
                                <div className="loadingSpinner"></div>
                            </div>
                        )}
                    </div>
                )}
                <div className="inputContainer">
                    <input
                        type="file"
                        id="fileUpload"
                        multiple
                        accept="image/*"
                        onChange={handleFileSelect}
                        style={{ display: 'none' }}
                        disabled={uploadingFiles || isLoading}
                    />
                    <label
                        htmlFor="fileUpload"
                        className={`fileUploadBtn ${uploadingFiles || isLoading ? 'disabled' : ''}`}
                        title="Attach images"
                    >
                        📎
                    </label>
                    <input
                        type="text"
                        name="text"
                        placeholder={placeholder}
                        disabled={isLoading || uploadingFiles}
                    />
                    <button
                        type="submit"
                        disabled={isLoading || uploadingFiles}
                        title={uploadingFiles ? "Uploading images..." : "Send message"}
                    >
                        {uploadingFiles ? (
                            <div className="loadingSpinner small"></div>
                        ) : (
                            <img src="/arrow.png" alt="Send" />
                        )}
                    </button>
                </div>
            </form>
        </div>
    );
};

export default ChatInput;
