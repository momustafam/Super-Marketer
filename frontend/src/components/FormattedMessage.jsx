import React from 'react';
import ReactMarkdown from 'react-markdown';
import './FormattedMessage.css';

const FormattedMessage = ({ content, isUser = false }) => {
    // If it's a user message, just display as plain text
    if (isUser) {
        return <div className="messageContent">{content}</div>;
    }

    // For AI responses, parse as markdown
    return (
        <div className="messageContent formatted">
            <ReactMarkdown
                components={{
                    // Custom renderers for different markdown elements
                    h1: ({ children }) => <h1 className="markdown-h1">{children}</h1>,
                    h2: ({ children }) => <h2 className="markdown-h2">{children}</h2>,
                    h3: ({ children }) => <h3 className="markdown-h3">{children}</h3>,
                    h4: ({ children }) => <h4 className="markdown-h4">{children}</h4>,
                    h5: ({ children }) => <h5 className="markdown-h5">{children}</h5>,
                    h6: ({ children }) => <h6 className="markdown-h6">{children}</h6>,
                    p: ({ children }) => <p className="markdown-p">{children}</p>,
                    strong: ({ children }) => <strong className="markdown-strong">{children}</strong>,
                    em: ({ children }) => <em className="markdown-em">{children}</em>,
                    ul: ({ children }) => <ul className="markdown-ul">{children}</ul>,
                    ol: ({ children }) => <ol className="markdown-ol">{children}</ol>,
                    li: ({ children }) => <li className="markdown-li">{children}</li>,
                    blockquote: ({ children }) => <blockquote className="markdown-blockquote">{children}</blockquote>,
                    code: ({ inline, children }) =>
                        inline ?
                            <code className="markdown-code-inline">{children}</code> :
                            <code className="markdown-code-block">{children}</code>,
                    pre: ({ children }) => <pre className="markdown-pre">{children}</pre>,
                    a: ({ href, children }) => (
                        <a href={href} className="markdown-link" target="_blank" rel="noopener noreferrer">
                            {children}
                        </a>
                    ),
                    hr: () => <hr className="markdown-hr" />,
                }}
            >
                {content}
            </ReactMarkdown>
        </div>
    );
};

export default FormattedMessage;
