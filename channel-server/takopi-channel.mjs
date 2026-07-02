#!/usr/bin/env node
import http from 'node:http';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';

const PORT = Number(process.env.TAKOPI_CHANNEL_PORT || '8788');
const HOST = process.env.TAKOPI_CHANNEL_HOST || '127.0.0.1';
const REPLY_URL = process.env.TAKOPI_REPLY_URL || 'http://127.0.0.1:8789/reply';
const SECRET = process.env.TAKOPI_CHANNEL_SECRET || '';

const mcp = new Server(
  { name: 'takopi', version: '0.0.1' },
  {
    capabilities: {
      experimental: { 'claude/channel': {} },
      tools: {},
    },
    instructions: `You receive Telegram messages from Takopi as <channel source="takopi" ...> events. Reply to each user-facing chat message by calling the takopi.reply tool with chat_id, reply_to_message_id, thread_id, and text. Keep replies concise unless asked. Preserve the project context from the metadata.

Format user-facing replies as Telegram-compatible Markdown:
- use short **bold** section labels for structured answers;
- use bullet lists for grouped facts, risks, and next steps;
- use fenced code blocks for logs, commands, diffs, JSON, stack traces, and other preformatted text;
- use Markdown tables only when they improve comparison;
- do not describe these formatting rules to the user.`,
  },
);

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'reply',
      description: 'Send a text reply back to the Telegram chat through Takopi.',
      inputSchema: {
        type: 'object',
        properties: {
          chat_id: { type: 'string' },
          reply_to_message_id: { type: 'string' },
          thread_id: { type: 'string' },
          text: { type: 'string' },
        },
        required: ['chat_id', 'text'],
      },
    },
  ],
}));

mcp.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;
  if (name !== 'reply') throw new Error(`Unknown tool: ${name}`);
  const res = await fetch(REPLY_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(SECRET ? { authorization: `Bearer ${SECRET}` } : {}),
    },
    body: JSON.stringify(args),
  });
  if (!res.ok) throw new Error(`Takopi reply failed: ${res.status} ${await res.text()}`);
  return { content: [{ type: 'text', text: 'sent' }] };
});

await mcp.connect(new StdioServerTransport());

const server = http.createServer(async (req, res) => {
  try {
    if (req.method !== 'POST' || req.url !== '/push') {
      res.writeHead(404).end('not found');
      return;
    }
    if (SECRET && req.headers.authorization !== `Bearer ${SECRET}`) {
      res.writeHead(401).end('unauthorized');
      return;
    }
    let body = '';
    for await (const chunk of req) body += chunk;
    const payload = JSON.parse(body || '{}');
    const meta = {
      chat_id: String(payload.chat_id || ''),
      message_id: String(payload.message_id || ''),
      thread_id: String(payload.thread_id || ''),
      project: String(payload.project || ''),
      branch: String(payload.branch || ''),
      engine: String(payload.engine || 'claude'),
    };
    const content = String(payload.text || '');
    await mcp.notification({
      method: 'notifications/claude/channel',
      params: { content, meta },
    });
    res.writeHead(200, { 'content-type': 'text/plain' }).end('ok');
  } catch (err) {
    console.error('push failed', err);
    res.writeHead(500).end(String(err?.message || err));
  }
});
server.listen(PORT, HOST, () => console.error(`takopi channel listening on ${HOST}:${PORT}`));
