import express from 'express';
import { fal } from '@fal-ai/client';

// === 新增：读取并处理多个FAL_KEY及自定义鉴权秘钥 ===
const FAL_KEYS = process.env.FAL_KEY ? process.env.FAL_KEY.split(',').map(key => key.trim()).filter(Boolean) : [];
const AUTH_SECRET = process.env.AUTH_SECRET;

if (FAL_KEYS.length === 0) {
    console.error("Error: FAL_KEY environment variable is not set or contains no valid keys.");
    process.exit(1);
}

if (!AUTH_SECRET) {
    console.error("Error: AUTH_SECRET environment variable is not set. This is required for authentication.");
    process.exit(1);
}

// 新增：随机选择一个FAL_KEY
function getRandomFalKey() {
    return FAL_KEYS[Math.floor(Math.random() * FAL_KEYS.length)];
}

const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));

const PORT = process.env.PORT || 3000;

// === 全局定义限制 ===
const PROMPT_LIMIT = 4800;
const SYSTEM_PROMPT_LIMIT = 4800;
// === 限制定义结束 ===

// 定义 fal-ai/any-llm 支持的模型列表
const FAL_SUPPORTED_MODELS = [
    "anthropic/claude-3.7-sonnet",
    "anthropic/claude-3.5-sonnet",
    "anthropic/claude-3-5-haiku",
    "anthropic/claude-3-haiku",
    "google/gemini-pro-1.5",
    "google/gemini-flash-1.5",
    "google/gemini-flash-1.5-8b",
    "google/gemini-2.0-flash-001",
    "meta-llama/llama-3.2-1b-instruct",
    "meta-llama/llama-3.2-3b-instruct",
    "meta-llama/llama-3.1-8b-instruct",
    "meta-llama/llama-3.1-70b-instruct",
    "openai/gpt-4o-mini",
    "openai/gpt-4o",
    "deepseek/deepseek-r1",
    "meta-llama/llama-4-maverick",
    "meta-llama/llama-4-scout"
];

// 新增：支持多模态的模型列表
const MULTIMODAL_MODELS = [
    "anthropic/claude-3.7-sonnet",
    "anthropic/claude-3.5-sonnet",
    "anthropic/claude-3-haiku",
    "google/gemini-pro-1.5",
    "openai/gpt-4o-mini",
    "openai/gpt-4o"
];

// === 修改：鉴权中间件使用Bearer认证 ===
const authenticateRequest = (req, res, next) => {
    // 从Authorization头部中获取Bearer token
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.startsWith('Bearer ') 
        ? authHeader.substring(7) // 移除"Bearer "前缀
        : null;
    
    // 仍然支持查询参数认证作为备选
    const queryToken = req.query.auth_secret;
    
    // 使用Authorization头部或查询参数中的token
    const authToken = token || queryToken;
    
    if (!authToken || authToken !== AUTH_SECRET) {
        console.warn("Authentication failed: Invalid or missing Bearer token");
        return res.status(401).json({ 
            error: 'Unauthorized',
            message: 'Authentication required. Use Authorization: Bearer <token>' 
        });
    }
    
    next();
};

// Helper function to get owner from model ID
const getOwner = (modelId) => {
    if (modelId && modelId.includes('/')) {
        return modelId.split('/')[0];
    }
    return 'fal-ai';
}

// 应用鉴权中间件到所有API路由
app.use('/v1', authenticateRequest);

// GET /v1/models endpoint
app.get('/v1/models', (req, res) => {
    console.log("Received request for GET /v1/models");
    try {
        const modelsData = FAL_SUPPORTED_MODELS.map(modelId => ({
            id: modelId, object: "model", created: 1700000000, owned_by: getOwner(modelId)
        }));
        res.json({ object: "list", data: modelsData });
        console.log("Successfully returned model list.");
    } catch (error) {
        console.error("Error processing GET /v1/models:", error);
        res.status(500).json({ error: "Failed to retrieve model list." });
    }
});

// === 新增：检测模型是否支持多模态 ===
function supportsMultimodal(modelId) {
    return MULTIMODAL_MODELS.includes(modelId);
}

// === 新增：处理图像转换为fal格式的函数 ===
function processImageForFal(imageUrl, imageBase64) {
    // fal-ai图像格式接受url或base64
    if (imageUrl) {
        return { url: imageUrl };
    } else if (imageBase64) {
        // 确保base64前缀符合要求
        let finalBase64 = imageBase64;
        if (finalBase64.startsWith('data:')) {
            // 从data URI中提取实际的base64部分
            const dataUriParts = finalBase64.split(',');
            if (dataUriParts.length > 1) {
                finalBase64 = dataUriParts[1];
            }
        }
        return { base64: finalBase64 };
    }
    return null;
}

// === 修改：支持多模态消息的转换函数 ===
function convertMessagesToFalPrompt(messages, modelId) {
    let fixed_system_prompt_content = "";
    const conversation_message_blocks = [];
    const isMultiModal = supportsMultimodal(modelId);
    
    console.log(`Original messages count: ${messages.length}, Model supports multimodal: ${isMultiModal}`);

    // 特殊处理多模态输入
    if (isMultiModal && modelId.startsWith("anthropic/claude")) {
        // 为Claude模型构建多模态输入
        const anthropicMessages = [];
        let hasSystemMessage = false;

        for (const message of messages) {
            if (message.role === 'system') {
                // Claude使用单独的system参数
                fixed_system_prompt_content += String(message.content || "");
                hasSystemMessage = true;
                continue;
            }
            
            // 处理user或assistant消息
            let anthropicMessage = {
                role: message.role === 'assistant' ? 'assistant' : 'user',
                content: []
            };
            
            // 处理消息内容（可能是字符串或内容数组）
            if (Array.isArray(message.content)) {
                // 处理消息内容数组（包含文本和图像）
                for (const part of message.content) {
                    if (part.type === 'text') {
                        anthropicMessage.content.push({
                            type: 'text',
                            text: part.text || ""
                        });
                    } else if (part.type === 'image_url') {
                        // 处理图像URL部分
                        let imageData = null;
                        if (typeof part.image_url === 'string') {
                            imageData = processImageForFal(part.image_url, null);
                        } else if (part.image_url && part.image_url.url) {
                            imageData = processImageForFal(part.image_url.url, null);
                        } else if (part.image_url && part.image_url.base64) {
                            imageData = processImageForFal(null, part.image_url.base64);
                        }
                        
                        if (imageData) {
                            anthropicMessage.content.push({
                                type: 'image',
                                source: imageData
                            });
                        }
                    }
                }
            } else if (typeof message.content === 'string' || message.content === null) {
                // 简单字符串内容
                anthropicMessage.content.push({
                    type: 'text',
                    text: String(message.content || "")
                });
            }
            
            anthropicMessages.push(anthropicMessage);
        }
        
        // 构建适合fal的格式
        // 根据fal-ai对Claude的要求，返回适合的结构
        return {
            system_prompt: fixed_system_prompt_content.trim(),
            anthropic_messages: anthropicMessages,
            // 标记这是Claude多模态格式
            is_claude_multimodal: true
        };
    } else if (isMultiModal) {
        // 其他多模态模型的处理（如Gemini、GPT-4）
        // 这里简化处理，实际项目可能需要针对每个模型定制
        console.log("Using generic multimodal format for non-Claude model");
        
        // 为其他模型构建多模态输入（简化格式，可能需要针对不同模型调整）
        const multimodalMessages = [];
        
        for (const message of messages) {
            if (message.role === 'system') {
                fixed_system_prompt_content += String(message.content || "");
                continue;
            }
            
            // 创建通用多模态消息格式
            let genericMessage = {
                role: message.role,
                content: []
            };
            
            if (Array.isArray(message.content)) {
                for (const part of message.content) {
                    if (part.type === 'text') {
                        genericMessage.content.push({
                            type: 'text',
                            text: part.text || ""
                        });
                    } else if (part.type === 'image_url') {
                        // 处理图像
                        let imageData = null;
                        if (typeof part.image_url === 'string') {
                            imageData = processImageForFal(part.image_url, null);
                        } else if (part.image_url && part.image_url.url) {
                            imageData = processImageForFal(part.image_url.url, null);
                        } else if (part.image_url && part.image_url.base64) {
                            imageData = processImageForFal(null, part.image_url.base64);
                        }
                        
                        if (imageData) {
                            genericMessage.content.push({
                                type: 'image',
                                source: imageData
                            });
                        }
                    }
                }
            } else if (typeof message.content === 'string' || message.content === null) {
                genericMessage.content.push({
                    type: 'text',
                    text: String(message.content || "")
                });
            }
            
            multimodalMessages.push(genericMessage);
        }
        
        return {
            system_prompt: fixed_system_prompt_content.trim(),
            generic_multimodal_messages: multimodalMessages,
            is_generic_multimodal: true
        };
    }

    // 1. 分离 System 消息，格式化 User/Assistant 消息（原始文本处理逻辑不变）
    for (const message of messages) {
        let content = (message.content === null || message.content === undefined) ? "" : String(message.content);
        switch (message.role) {
            case 'system':
                fixed_system_prompt_content += `System: ${content}\n\n`;
                break;
            case 'user':
                conversation_message_blocks.push(`Human: ${content}\n\n`);
                break;
            case 'assistant':
                conversation_message_blocks.push(`Assistant: ${content}\n\n`);
                break;
            default:
                console.warn(`Unsupported role: ${message.role}`);
                continue;
        }
    }

    // 2. 截断合并后的 system 消息（如果超长）
    if (fixed_system_prompt_content.length > SYSTEM_PROMPT_LIMIT) {
        const originalLength = fixed_system_prompt_content.length;
        fixed_system_prompt_content = fixed_system_prompt_content.substring(0, SYSTEM_PROMPT_LIMIT);
        console.warn(`Combined system messages truncated from ${originalLength} to ${SYSTEM_PROMPT_LIMIT}`);
    }
    // 清理末尾可能多余的空白，以便后续判断和拼接
    fixed_system_prompt_content = fixed_system_prompt_content.trim();

    // 3. 计算 system_prompt 中留给对话历史的剩余空间
    let space_occupied_by_fixed_system = 0;
    if (fixed_system_prompt_content.length > 0) {
        space_occupied_by_fixed_system = fixed_system_prompt_content.length + 4;
    }
    const remaining_system_limit = Math.max(0, SYSTEM_PROMPT_LIMIT - space_occupied_by_fixed_system);
    console.log(`Trimmed fixed system prompt length: ${fixed_system_prompt_content.length}. Approx remaining system history limit: ${remaining_system_limit}`);

    // 4. 反向填充 User/Assistant 对话历史
    const prompt_history_blocks = [];
    const system_prompt_history_blocks = [];
    let current_prompt_length = 0;
    let current_system_history_length = 0;
    let promptFull = false;
    let systemHistoryFull = (remaining_system_limit <= 0);

    console.log(`Processing ${conversation_message_blocks.length} user/assistant messages for recency filling.`);
    for (let i = conversation_message_blocks.length - 1; i >= 0; i--) {
        const message_block = conversation_message_blocks[i];
        const block_length = message_block.length;

        if (promptFull && systemHistoryFull) {
            console.log(`Both prompt and system history slots full. Omitting older messages from index ${i}.`);
            break;
        }

        // 优先尝试放入 prompt
        if (!promptFull) {
            if (current_prompt_length + block_length <= PROMPT_LIMIT) {
                prompt_history_blocks.unshift(message_block);
                current_prompt_length += block_length;
                continue;
            } else {
                promptFull = true;
                console.log(`Prompt limit (${PROMPT_LIMIT}) reached. Trying system history slot.`);
            }
        }

        // 如果 prompt 满了，尝试放入 system_prompt 的剩余空间
        if (!systemHistoryFull) {
            if (current_system_history_length + block_length <= remaining_system_limit) {
                system_prompt_history_blocks.unshift(message_block);
                current_system_history_length += block_length;
                continue;
            } else {
                systemHistoryFull = true;
                console.log(`System history limit (${remaining_system_limit}) reached.`);
            }
        }
    }

    // 5. 组合最终的 prompt 和 system_prompt (包含分隔符逻辑)
    const system_prompt_history_content = system_prompt_history_blocks.join('').trim();
    const final_prompt = prompt_history_blocks.join('').trim();

    // 定义分隔符
    const SEPARATOR = "\n\n-------下面是比较早之前的对话内容-----\n\n";

    let final_system_prompt = "";

    // 检查各部分是否有内容
    const hasFixedSystem = fixed_system_prompt_content.length > 0;
    const hasSystemHistory = system_prompt_history_content.length > 0;

    if (hasFixedSystem && hasSystemHistory) {
        // 两部分都有，用分隔符连接
        final_system_prompt = fixed_system_prompt_content + SEPARATOR + system_prompt_history_content;
        console.log("Combining fixed system prompt and history with separator.");
    } else if (hasFixedSystem) {
        // 只有固定部分
        final_system_prompt = fixed_system_prompt_content;
        console.log("Using only fixed system prompt.");
    } else if (hasSystemHistory) {
        // 只有历史部分 (固定部分为空)
        final_system_prompt = system_prompt_history_content;
        console.log("Using only history in system prompt slot.");
    }

    // 6. 返回结果
    const result = {
        system_prompt: final_system_prompt,
        prompt: final_prompt
    };

    console.log(`Final system_prompt length (Sys+Separator+Hist): ${result.system_prompt.length}`);
    console.log(`Final prompt length (Hist): ${result.prompt.length}`);

    return result;
}

// ======= 处理流式响应并收集完整输出的函数 =======
async function collectStreamedResponse(falStream) {
    let completeOutput = '';
    let lastEventData = null;
    let reasoningOutput = null;

    try {
        for await (const event of falStream) {
            // 保存最后一个事件的数据
            lastEventData = event;
            
            // 如果存在output，累积到completeOutput
            if (event && typeof event.output === 'string') {
                completeOutput = event.output; // 直接使用完整输出，因为fal总是发送累积结果
            }
            
            // 如果有reasoning字段，保存
            if (event && event.reasoning) {
                reasoningOutput = event.reasoning;
            }
            
            // 检查是否完成 (partial=false表示完成)
            if (event && event.partial === false) {
                break;
            }
        }
        
        return {
            output: completeOutput,
            requestId: lastEventData?.request_id || `manual-${Date.now()}`,
            reasoning: reasoningOutput
        };
    } catch (error) {
        console.error('Error collecting streamed response:', error);
        throw error;
    }
}

// POST /v1/chat/completions endpoint (修改了处理逻辑，支持多模态)
app.post('/v1/chat/completions', async (req, res) => {
    const { model, messages, stream = false, reasoning = false, ...restOpenAIParams } = req.body;

    console.log(`Received chat completion request for model: ${model}, stream: ${stream}`);

    if (!FAL_SUPPORTED_MODELS.includes(model)) {
        console.warn(`Warning: Requested model '${model}' is not in the explicitly supported list.`);
    }
    if (!model || !messages || !Array.isArray(messages) || messages.length === 0) {
        console.error("Invalid request parameters:", { model, messages: Array.isArray(messages) ? messages.length : typeof messages });
        return res.status(400).json({ error: 'Missing or invalid parameters: model and messages array are required.' });
    }

    try {
        // === 随机选择一个FAL_KEY并配置 ===
        const selectedKey = getRandomFalKey();
        // 重新配置fal客户端使用随机选择的密钥
        fal.config({
            credentials: selectedKey,
        });
        console.log(`Using randomly selected FAL key: ${selectedKey.substring(0, 5)}...`);

        // 检查是否使用多模态模型
        const isMultiModal = supportsMultimodal(model);
        console.log(`Model ${model} multimodal support: ${isMultiModal}`);

        // 使用更新后的转换函数，传入模型ID
        const convertedInput = convertMessagesToFalPrompt(messages, model);

        // 构建fal-ai请求参数
        let falInput = {};
        
        if (convertedInput.is_claude_multimodal) {
            // Claude多模态格式
            falInput = {
                model: model,
                system: convertedInput.system_prompt || "",
                messages: convertedInput.anthropic_messages,
                reasoning: !!reasoning,
            };
            console.log("Using Claude multimodal format");
        } else if (convertedInput.is_generic_multimodal) {
            // 其他多模态模型格式
            falInput = {
                model: model,
                system_prompt: convertedInput.system_prompt || "",
                messages: convertedInput.generic_multimodal_messages,
                reasoning: !!reasoning,
            };
            console.log("Using generic multimodal format");
        } else {
            // 使用原有的文本格式
            falInput = {
                model: model,
                prompt: convertedInput.prompt,
                ...(convertedInput.system_prompt && { system_prompt: convertedInput.system_prompt }),
                reasoning: !!reasoning,
            };
            console.log("Using text-only format");
        }

        console.log("Fal Input Structure:", 
            JSON.stringify({
                model: falInput.model,
                has_system: !!falInput.system || !!falInput.system_prompt,
                has_messages: !!falInput.messages,
                has_prompt: !!falInput.prompt,
                is_multimodal: isMultiModal,
                reasoning: falInput.reasoning
            }, null, 2)
        );

        // --- 流式/非流式处理逻辑 ---
        if (stream) {
            // === 流式处理逻辑（保持不变） ===
            res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
            res.setHeader('Cache-Control', 'no-cache');
            res.setHeader('Connection', 'keep-alive');
            res.setHeader('Access-Control-Allow-Origin', '*');
            res.flushHeaders();

            let previousOutput = '';

            const falStream = await fal.stream("fal-ai/any-llm", { input: falInput });

            try {
                for await (const event of falStream) {
                    const currentOutput = (event && typeof event.output === 'string') ? event.output : '';
                    const isPartial = (event && typeof event.partial === 'boolean') ? event.partial : true;
                    const errorInfo = (event && event.error) ? event.error : null;

                    if (errorInfo) {
                        console.error("Error received in fal stream event:", errorInfo);
                        const errorChunk = { id: `chatcmpl-${Date.now()}-error`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: model, choices: [{ index: 0, delta: {}, finish_reason: "error", message: { role: 'assistant', content: `Fal Stream Error: ${JSON.stringify(errorInfo)}` } }] };
                        res.write(`data: ${JSON.stringify(errorChunk)}\n\n`);
                        break;
                    }

                    let deltaContent = '';
                    if (currentOutput.startsWith(previousOutput)) {
                        deltaContent = currentOutput.substring(previousOutput.length);
                    } else if (currentOutput.length > 0) {
                        console.warn("Fal stream output mismatch detected. Sending full current output as delta.", { previousLength: previousOutput.length, currentLength: currentOutput.length });
                        deltaContent = currentOutput;
                        previousOutput = '';
                    }
                    previousOutput = currentOutput;

                    if (deltaContent || !isPartial) {
                        const openAIChunk = { id: `chatcmpl-${Date.now()}`, object: "chat.completion.chunk", created: Math.floor(Date.now() / 1000), model: model, choices: [{ index: 0, delta: { content: deltaContent }, finish_reason: isPartial === false ? "stop" : null }] };
                        res.write(`data: ${JSON.stringify(openAIChunk)}\n\n`);
                    }
                }
                res.write(`data: [DONE]\n\n`);
                res.end();
                console.log("Stream finished.");

            } catch (streamError) {
                console.error('Error during fal stream processing loop:', streamError);
                try {
                    const errorDetails = (streamError instanceof Error) ? streamError.message : JSON.stringify(streamError);
                    res.write(`data: ${JSON.stringify({ error: { message: "Stream processing error", type: "proxy_error", details: errorDetails } })}\n\n`);
                    res.write(`data: [DONE]\n\n`);
                    res.end();
                } catch (finalError) {
                    console.error('Error sending stream error message to client:', finalError);
                    if (!res.writableEnded) { res.end(); }
                }
            }
        } else {
            // === 非流式处理逻辑 ===
            console.log("Executing non-stream request through stream API with collection...");
            
            try {
                // 获取流式响应并收集完整输出
                const falStream = await fal.stream("fal-ai/any-llm", { input: falInput });
                const collectedResult = await collectStreamedResponse(falStream);
                
                console.log("Collected complete non-stream result:", JSON.stringify({
                    output_length: collectedResult.output?.length,
                    requestId: collectedResult.requestId,
                    has_reasoning: !!collectedResult.reasoning
                }));

                // 构造OpenAI格式的响应
                const openAIResponse = {
                    id: `chatcmpl-${collectedResult.requestId || Date.now()}`, 
                    object: "chat.completion", 
                    created: Math.floor(Date.now() / 1000), 
                    model: model,
                    choices: [{ 
                        index: 0, 
                        message: { 
                            role: "assistant", 
                            content: collectedResult.output || "" 
                        }, 
                        finish_reason: "stop" 
                    }],
                    usage: { prompt_tokens: null, completion_tokens: null, total_tokens: null }, 
                    system_fingerprint: null,
                    ...(collectedResult.reasoning && { fal_reasoning: collectedResult.reasoning }),
                };
                
                res.json(openAIResponse);
                console.log("Returned collected non-stream response.");
            } catch (error) {
                console.error('Error in non-stream mode using stream collection:', error);
                const errorMessage = (error instanceof Error) ? error.message : JSON.stringify(error);
                res.status(500).json({ 
                    object: "error", 
                    message: `Error collecting non-stream response: ${errorMessage}`, 
                    type: "proxy_error", 
                    param: null, 
                    code: null 
                });
            }
        }

    } catch (error) {
        console.error('Unhandled error in /v1/chat/completions:', error);
        if (!res.headersSent) {
            const errorMessage = (error instanceof Error) ? error.message : JSON.stringify(error);
            res.status(500).json({ error: 'Internal Server Error in Proxy', details: errorMessage });
        } else if (!res.writableEnded) {
             console.error("Headers already sent, ending response.");
             res.end();
        }
    }
});

// === 添加CORS支持和OPTIONS请求处理 ===
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});

// 启动服务器
app.listen(PORT, () => {
    console.log(`===================================================`);
    console.log(` Fal OpenAI Proxy Server (多模态支持版本)`); 
    console.log(` Listening on port: ${PORT}`);
    console.log(` Using Limits: System Prompt=${SYSTEM_PROMPT_LIMIT}, Prompt=${PROMPT_LIMIT}`);
    console.log(` Fal AI Keys Loaded: ${FAL_KEYS.length}`);
    console.log(` Authentication Required: Yes (via Bearer Token)`);
    console.log(` Chat Completions Endpoint: POST http://localhost:${PORT}/v1/chat/completions`);
    console.log(` Models Endpoint: GET http://localhost:${PORT}/v1/models`);
    console.log(` Multimodal Support: Enabled (${MULTIMODAL_MODELS.length} models)`);
    console.log(`===================================================`);
});

// 根路径响应
app.get('/', (req, res) => {
    res.send('Fal OpenAI Proxy (多模态支持版本) is running. Authentication required using Bearer token.');
});
