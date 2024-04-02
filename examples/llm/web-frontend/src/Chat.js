import {useState} from "react";
import {
    gql,
    useMutation,
    useLazyQuery,
    useSubscription, useQuery,
} from "@apollo/client";
import tw from "tailwind-styled-components";

import styles from "@chatscope/chat-ui-kit-styles/dist/default/styles.min.css";
import {
    MainContainer,
    ChatContainer,
    MessageList,
    Message,
    MessageInput, ConversationList, Conversation, Avatar, ConversationHeader, InputToolbox, TypingIndicator,
} from "@chatscope/chat-ui-kit-react";

const PROMPT = gql`
query Prompt($prompt: string) {
  prompt(prompt: $prompt)
}
`;

function Prompt(prompt) {
    let {
        data: data,
        called: called,
        error: error
    } = useQuery(PROMPT, {
        fetchPolicy: "network-only",
        variables: {prompt: `${prompt}`},
    });

    if (error != null) {
        console.log(error);
    }

    return data;
}

function handleSend(message, messages, setMessages, setTypingIndicator) {
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
    setTypingIndicator(<TypingIndicator content="LineraGPT is thinking..."/>)
}

function Chat({chainId}) {
    let initial_messages = (
        [{
            props: {
                model: {
                    message: "Hey! I'm LineraGPT. How can I assist you?",
                    sender: "LineraGPT",
                    direction: "incoming",
                    position: "single"
                }
            }
        }])
    ;
    const [messages, setMessages] = useState(initial_messages);
    const [typingIndicator, setTypingIndicator] = useState(null);

    return (
        <div>
            <MainContainer style={{height: '100vh', response: true}}>
                <ChatContainer>
                    <ConversationHeader>
                        {/*<TODO: put Linera logo here*/}
                        {/*<Avatar*/}
                        {/*    name="LineraGPT"*/}
                        {/*    src="localhost:3000/public/favicon.ico"*/}
                        {/*/>*/}
                        <ConversationHeader.Content
                            info="Online"
                            userName="LineraGPT"
                        />
                    </ConversationHeader>
                    <MessageList typingIndicator={typingIndicator}>
                        {messages.map((m, i) => <Message key={i} {...m.props} />)}
                    </MessageList>
                    <MessageInput placeholder="Type message here"
                                  onSend={(innerHtml, textContent, innerText, nodes) => handleSend(textContent, messages, setMessages, setTypingIndicator)}/>
                </ChatContainer>
            </MainContainer>
        </div>
    )
}

export default Chat;