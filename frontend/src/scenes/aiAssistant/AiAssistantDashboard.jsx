import { useMutation, useQueryClient } from "@tanstack/react-query";
import "./AiAssistantDashboard.css";
import { useNavigate } from "react-router-dom";
import { useState } from "react";
import Topbar from "../global/Topbar";
import Sidebar from "../global/Sidebar";
import ChatList from "../../components/ChatList";
import ChatInput from "../../components/ChatInput";
import { Box } from "@mui/material";

const DashboardPage = () => {
    const queryClient = useQueryClient();
    const navigate = useNavigate();
    const [isCollapsed, setIsCollapsed] = useState(false);

    const mutation = useMutation({
        mutationFn: (chatData) => {
            return fetch(`http://localhost:8001/api/chats`, {
                method: "POST",
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(chatData),
            }).then((res) => res.json());
        },
        onSuccess: (data) => {
            // Invalidate and refetch
            queryClient.invalidateQueries({ queryKey: ["userChats"] });
            console.log("Chat created successfully:", data);

            // Navigate to the new chat
            if (data.success && data.data.chat_id) {
                navigate(`/tools/ai-assistant/chats/${data.data.chat_id}`);
            }
        },
        onError: (error) => {
            console.error("Error creating chat:", error);
        }
    });

    const handleChatSubmit = async (chatData) => {
        mutation.mutate(chatData);
    };
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
                    <Box className="dashboardPage">
                        <div className="texts">
                            <div className="logo">
                                <h1>AI Assistant</h1>
                            </div>
                            <div className="options">
                                <div className="option">
                                    <img src="/chat.png" alt="" />
                                    <span>Create a New Chat</span>
                                </div>
                                <div className="option">
                                    <img src="/image.png" alt="" />
                                    <span>Analyze Images</span>
                                </div>
                                <div className="option">
                                    <img src="/code.png" alt="" />
                                    <span>Help me with campaign</span>
                                </div>
                            </div>
                        </div>
                        <div className="formContainer">
                            <ChatInput
                                onSubmit={handleChatSubmit}
                                isLoading={mutation.isPending}
                                placeholder="Ask me anything..."
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

export default DashboardPage;