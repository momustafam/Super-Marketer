import "./ChatPage.css";
import { useParams } from "react-router-dom";
import { useState, useEffect, useRef } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import Topbar from "../global/Topbar";
import Sidebar from "../global/Sidebar";
import ChatList from "../../components/ChatList";
import ChatInput from "../../components/ChatInput";
import { Box } from "@mui/material";

const ChatPage = () => {
    const { chatId } = useParams();
    const [isCollapsed, setIsCollapsed] = useState(false);
    const chatContainerRef = useRef(null);
    const queryClient = useQueryClient();

    // Fetch chat data from backend
    const { data: chatData, isLoading, error } = useQuery({
        queryKey: ['chat', chatId],
        queryFn: async () => {
            const response = await fetch(`http://localhost:8001/api/chats/${chatId}`);
            if (!response.ok) {
                throw new Error('Failed to fetch chat');
            }
            const result = await response.json();
            return result.data;
        },
        enabled: !!chatId
    });

    // Mutation for sending new messages
    const sendMessageMutation = useMutation({
        mutationFn: async (messageData) => {
            const response = await fetch(`http://localhost:8001/api/chats/${chatId}/messages`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(messageData),
            });
            if (!response.ok) {
                throw new Error('Failed to send message');
            }
            return response.json();
        },
        onSuccess: () => {
            // Invalidate and refetch chat data to get the new messages
            queryClient.invalidateQueries({ queryKey: ['chat', chatId] });
            queryClient.invalidateQueries({ queryKey: ['userChats'] });
        }
    });

    // Auto-scroll to bottom when messages change
    useEffect(() => {
        const scrollToBottom = () => {
            if (chatContainerRef.current) {
                chatContainerRef.current.scrollTop = chatContainerRef.current.scrollHeight;
            }
        };

        // Use setTimeout to ensure DOM is fully rendered
        const timeoutId = setTimeout(scrollToBottom, 100);

        return () => clearTimeout(timeoutId);
    }, [chatData?.messages]);

    const handleChatSubmit = async (submissionData) => {
        try {
            await sendMessageMutation.mutateAsync(submissionData);
        } catch (error) {
            console.error('Error sending message:', error);
        }
    };

    if (isLoading) {
        return (
            <Box className="aiAssistantContainer">
                <Box className="mainLayout">
                    <Box className={`sidebarContainer ${isCollapsed ? 'collapsed' : ''}`}>
                        <Sidebar
                            isSidebar={true}
                            isCollapsed={isCollapsed}
                            setIsCollapsed={setIsCollapsed}
                        />
                    </Box>
                    <Box className="centerSection">
                        <Topbar />
                        <Box className="chatPage">
                            <div className="chatHeader">
                                <h2>Loading...</h2>
                            </div>
                        </Box>
                    </Box>
                    <Box className="chatListContainer">
                        <ChatList />
                    </Box>
                </Box>
            </Box>
        );
    }

    if (error || !chatData) {
        return (
            <Box className="aiAssistantContainer">
                <Box className="mainLayout">
                    <Box className={`sidebarContainer ${isCollapsed ? 'collapsed' : ''}`}>
                        <Sidebar
                            isSidebar={true}
                            isCollapsed={isCollapsed}
                            setIsCollapsed={setIsCollapsed}
                        />
                    </Box>
                    <Box className="centerSection">
                        <Topbar />
                        <Box className="chatPage">
                            <div className="chatHeader">
                                <h2>Chat not found</h2>
                            </div>
                        </Box>
                    </Box>
                    <Box className="chatListContainer">
                        <ChatList />
                    </Box>
                </Box>
            </Box>
        );
    }

    return (
        <Box className="aiAssistantContainer">
            <Box className="mainLayout">
                <Box className={`sidebarContainer ${isCollapsed ? 'collapsed' : ''}`}>
                    <Sidebar
                        isSidebar={true}
                        isCollapsed={isCollapsed}
                        setIsCollapsed={setIsCollapsed}
                    />
                </Box>
                <Box className="centerSection">
                    <Topbar />
                    <Box className="chatPage">
                        <div className="chatHeader">
                            <h2>{chatData.title}</h2>
                        </div>
                        <div className="wrapper" ref={chatContainerRef}>
                            <div className="chat">
                                {chatData.messages && chatData.messages.map((message, index) => (
                                    <div
                                        key={message.id || index}
                                        className={`message ${message.role === 'user' ? 'user' : ''}`}
                                    >
                                        <div className="messageContent">
                                            {message.content}
                                        </div>
                                        {message.images && message.images.length > 0 && (
                                            <div className="messageImages">
                                                {message.images.map((image, imgIndex) => (
                                                    <div key={imgIndex} className="imageContainer">
                                                        <img
                                                            src={image.thumbnail_url || image.url}
                                                            alt={image.fileName}
                                                            className="chatImage"
                                                            onClick={() => window.open(image.url, '_blank')}
                                                            title={`Click to view full size - ${image.fileName}`}
                                                        />
                                                    </div>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                ))}
                            </div>
                        </div>
                        <div className="formContainer">
                            <ChatInput
                                onSubmit={handleChatSubmit}
                                isLoading={sendMessageMutation.isPending}
                                placeholder="Continue the conversation..."
                            />
                        </div>
                    </Box>
                </Box>
                <Box className="chatListContainer">
                    <ChatList />
                </Box>
            </Box>
        </Box>
    );
};

export default ChatPage;