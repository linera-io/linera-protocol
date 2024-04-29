import {useState} from "react";
import {
    gql,
    useLazyQuery,
} from "@apollo/client";

// eslint-disable-next-line
import styles from "@chatscope/chat-ui-kit-styles/dist/default/styles.min.css";


import {
    MainContainer,
    ChatContainer,
    MessageList,
    Message,
    MessageInput, ConversationHeader, TypingIndicator,
} from "@chatscope/chat-ui-kit-react";

const PROMPT = gql`
query Prompt($prompt: String) {
  prompt(prompt: $prompt)
}
`;

function handleSend(message, messages, setMessages, setTypingIndicator, doPrompt) {
    setMessages([...messages, {
        props: {
            model: {
                message: message,
                sender: "Me",
                direction: "outgoing",
                position: "single"
            }
        }
    }]);
    setTypingIndicator(<TypingIndicator content="Linera Stories is thinking..."/>)
    doPrompt({variables: {prompt: message}});
}

function Chat({chainId}) {
    let initial_messages = (
        [{
            props: {
                model: {
                    message: "Hey! I'm Linera Stories. Start a story and I'll finish it for you.",
                    sender: "Linera Stories",
                    direction: "incoming",
                    position: "single"
                }
            }
        }])
    ;
    const [messages, setMessages] = useState(initial_messages);
    const [typingIndicator, setTypingIndicator] = useState(null);

    const [doPrompt
    ] = useLazyQuery(PROMPT, {
        fetchPolicy: "network-only",
        onCompleted: (data) => {
            data.prompt.split("\n").forEach((s) => {
                setMessages(prevMessages => [...prevMessages, {
                    props: {
                        model: {
                            message: s,
                            sender: "Linera Stories",
                            direction: "incoming",
                            position: "single"
                        }
                    }
                }]);
            });
            setTypingIndicator(null)
        },
        onError: (error) => {
            console.log(error)
        }
    });

    return (
        <div>
            <MainContainer style={{height: '100vh', response: true}}>
                <ChatContainer>
                    <ConversationHeader>
                        <ConversationHeader.Content
                            info="Online"
                            userName="Linera Stories"
                        />
                    </ConversationHeader>
                    <MessageList typingIndicator={typingIndicator}>
                        {messages.map((m, i) => <Message key={i} {...m.props} />)}
                    </MessageList>
                    <MessageInput placeholder="Type message here"
                                  onSend={(innerHtml, textContent, innerText, nodes) => handleSend(textContent, messages, setMessages, setTypingIndicator, doPrompt)}/>
                </ChatContainer>
            </MainContainer>
        </div>
    )
}

export default Chat;