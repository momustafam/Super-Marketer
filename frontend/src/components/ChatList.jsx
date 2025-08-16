import './ChatList.css';
import { Link } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';

const ChatList = () => {
    // Fetch chats from the backend API
    const { data: chatsData, isLoading, error } = useQuery({
        queryKey: ['userChats'],
        queryFn: async () => {
            const response = await fetch('http://localhost:8001/api/chats');
            if (!response.ok) {
                throw new Error('Failed to fetch chats');
            }
            const result = await response.json();
            return result.data;
        }
    });

    const recentChats = chatsData?.chats || [];

    if (isLoading) {
        return (
            <div className="chat-list">
                <span className='title'>Loading...</span>
            </div>
        );
    }

    if (error) {
        console.error('Error fetching chats:', error);
    }

    return (
        <div className="chat-list">
            <span className='title'>New Chats</span>
            <Link to="/tools/ai-assistant">Create a new Chat</Link>
            <hr />
            <span className='title'>Recent Chats</span>
            <div className='list'>
                {recentChats.length > 0 ? (
                    recentChats.map((chat) => (
                        <Link
                            key={chat.id}
                            to={`/tools/ai-assistant/chats/${chat.id}`}
                            title={chat.title}
                        >
                            {chat.title}
                        </Link>
                    ))
                ) : (
                    <div className="no-chats">No recent chats</div>
                )}
            </div>
        </div>
    );
}

export default ChatList;