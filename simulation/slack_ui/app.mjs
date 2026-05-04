// Simulation harness — Slack authoring UI.
//
// React via ESM CDN, hyperscript-style elements (no JSX — avoids
// a build step). Runs against simulation/server.py. Pure client-side
// state; the only persistence happens via /simulation/inject.
//
// Structure:
//   <Sidebar>  persona switcher + channel list
//   <TopBar>   current persona + voice hints + time-shift
//   <Messages> last 20 observations in the selected channel
//   <Composer> textarea + Send
//
// Keyboard: Cmd/Ctrl+Enter submits the composer.

import { createElement as h, useEffect, useState, useCallback, useMemo } from 'react';
import { createRoot } from 'react-dom/client';

// Read demo tenant from URL ?tenant_id= so the simulator routes signals
// to the active demo session even though it runs on a different origin.
const _urlTenantId = new URLSearchParams(window.location.search).get('tenant_id');

const API = {
  personas: '/simulation/personas',
  channels: '/simulation/channels',
  messages: (ch) => `/simulation/messages?channel=${encodeURIComponent(ch)}`,
  inject: '/simulation/inject',
  health: '/simulation/health',
};

async function j(url, opts) {
  const res = await fetch(url, opts);
  if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
  return res.json();
}

function formatTime(iso) {
  try {
    const d = new Date(iso);
    return d.toISOString().replace('T', ' ').slice(0, 19) + 'Z';
  } catch {
    return iso;
  }
}

function Sidebar({ personas, activePersonaId, onPickPersona, channels, activeChannel, onPickChannel }) {
  return h('div', { className: 'sidebar' }, [
    h('h2', { key: 'h1' }, 'Author as'),
    ...personas.map((p) =>
      h(
        'button',
        {
          key: p.id,
          className: 'persona-item ' + (p.id === activePersonaId ? 'active' : ''),
          onClick: () => onPickPersona(p.id),
        },
        [
          h('div', { key: 'n' }, p.name),
          h('div', { className: 'role', key: 'r' }, `${p.role} · ${p.slack_handle || 'no-slack'}`),
        ],
      ),
    ),
    h('h2', { key: 'h2' }, 'Channels'),
    ...channels.map((c) =>
      h(
        'button',
        {
          key: c.handle,
          className: 'channel-item ' + (c.handle === activeChannel ? 'active' : ''),
          onClick: () => onPickChannel(c.handle),
        },
        [h('span', { className: 'hash', key: 'h' }, '#'), c.handle],
      ),
    ),
  ]);
}

function TopBar({ activePersona, timeShift, onTimeShiftChange, health }) {
  const hints = activePersona?.voice_hints || 'Pick a persona to start authoring.';
  return h('div', { className: 'topbar' }, [
    h('div', { className: 'current-persona', key: 'cp' }, activePersona?.name || '—'),
    h('div', { className: 'voice-hints', key: 'vh' }, hints),
    h('div', { className: 'time-shift', key: 'ts' }, [
      h(
        'span',
        {
          key: 'lbl',
          className: timeShift && timeShift !== 'now' ? 'shift-active' : '',
        },
        timeShift && timeShift !== 'now' ? 'SIMULATION TIME' : 'occurred_at',
      ),
      h('input', {
        key: 'inp',
        type: 'text',
        placeholder: 'now  |  -3h  |  2026-04-22T09:00:00Z',
        value: timeShift,
        onChange: (e) => onTimeShiftChange(e.target.value),
      }),
      h(
        'span',
        {
          key: 'health',
          style: { color: 'var(--fg-muted)', marginLeft: 12, fontSize: 11 },
        },
        health
          ? `tenant ${health.tenant_id.slice(0, 8)}… · run ${health.run_id.slice(0, 16)}…`
          : '',
      ),
    ]),
  ]);
}

function Messages({ messages, channel }) {
  if (!messages || !messages.length) {
    return h(
      'div',
      { className: 'messages' },
      h('div', { className: 'empty' }, `No messages yet in #${channel}.`),
    );
  }
  return h(
    'div',
    { className: 'messages' },
    messages.map((m) =>
      h('div', { className: 'message', key: m.observation_id }, [
        h('div', { className: 'meta', key: 'm' }, [
          h('span', { className: 'author', key: 'a' }, m.author_handle || '(no-actor)'),
          h('span', { key: 't' }, formatTime(m.occurred_at)),
          m.scenario_id
            ? h(
                'span',
                { key: 's', style: { marginLeft: 8, color: 'var(--warn)' } },
                `scenario=${m.scenario_id}`,
              )
            : null,
        ]),
        h('div', { className: 'body', key: 'b' }, m.content_text),
      ]),
    ),
  );
}

function Composer({ disabled, onSend }) {
  const [text, setText] = useState('');
  const [status, setStatus] = useState(null);
  const submit = useCallback(async () => {
    if (!text.trim()) return;
    setStatus({ kind: 'pending', msg: 'Injecting…' });
    try {
      const res = await onSend(text);
      setStatus({ kind: 'ok', msg: `obs ${res.observation_id.slice(0, 8)}… ${res.deduped ? '(deduped)' : ''}` });
      setText('');
    } catch (e) {
      setStatus({ kind: 'err', msg: String(e.message || e) });
    }
  }, [text, onSend]);
  const onKey = useCallback(
    (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') submit();
    },
    [submit],
  );
  return h('div', { className: 'composer' }, [
    h('textarea', {
      key: 'ta',
      value: text,
      placeholder: 'Type as the selected persona. Cmd/Ctrl+Enter to send.',
      onChange: (e) => setText(e.target.value),
      onKeyDown: onKey,
      disabled,
    }),
    h('div', { className: 'row', key: 'r' }, [
      h('div', { className: 'hint', key: 'h' }, 'POST /simulation/inject'),
      h(
        'div',
        {
          className: status ? `hint ${status.kind === 'err' ? 'error' : 'ok'}` : 'hint',
          key: 's',
        },
        status ? status.msg : '',
      ),
      h('button', { key: 'b', disabled: disabled || !text.trim(), onClick: submit }, 'Send'),
    ]),
  ]);
}

function App() {
  const [personas, setPersonas] = useState([]);
  const [channels, setChannels] = useState([]);
  const [activePersonaId, setActivePersonaId] = useState(null);
  const [activeChannel, setActiveChannel] = useState(null);
  const [messages, setMessages] = useState([]);
  const [timeShift, setTimeShift] = useState('now');
  const [health, setHealth] = useState(null);
  const [bootError, setBootError] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        const [pResp, cResp, hResp] = await Promise.all([
          j(API.personas),
          j(API.channels),
          j(API.health),
        ]);
        setPersonas(pResp.personas);
        setChannels(cResp.channels);
        setHealth(hResp);
        if (!activePersonaId && pResp.personas.length) {
          setActivePersonaId(pResp.personas[0].id);
        }
        if (!activeChannel && cResp.channels.length) {
          setActiveChannel(cResp.channels[0].handle);
        }
      } catch (e) {
        setBootError(String(e.message || e));
      }
    })();
    // eslint-disable-next-line
  }, []);

  const refreshMessages = useCallback(async () => {
    if (!activeChannel) return;
    try {
      const r = await j(API.messages(activeChannel));
      setMessages(r.messages);
    } catch (e) {
      console.warn('messages fetch failed', e);
    }
  }, [activeChannel]);

  useEffect(() => {
    refreshMessages();
  }, [refreshMessages]);

  const activePersona = useMemo(
    () => personas.find((p) => p.id === activePersonaId),
    [personas, activePersonaId],
  );

  const onSend = useCallback(
    async (text) => {
      const body = {
        persona: activePersonaId,
        channel: activeChannel,
        content_text: text,
        occurred_at: timeShift || 'now',
        ...(_urlTenantId ? { tenant_id: _urlTenantId } : {}),
      };
      const res = await j(API.inject, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
      });
      await refreshMessages();
      return res;
    },
    [activePersonaId, activeChannel, timeShift, refreshMessages],
  );

  if (bootError) {
    return h('div', { style: { padding: 40 } }, [
      h('h1', { key: 't' }, 'Simulation UI — boot error'),
      h('pre', { key: 'p' }, bootError),
      h(
        'p',
        { key: 'm' },
        'Make sure uvicorn is running with COMPANY_OS_ENV=dev and DATABASE_URL set.',
      ),
    ]);
  }

  return h('div', { className: 'layout' }, [
    h(Sidebar, {
      key: 's',
      personas,
      activePersonaId,
      onPickPersona: setActivePersonaId,
      channels,
      activeChannel,
      onPickChannel: setActiveChannel,
    }),
    h('div', { className: 'main', key: 'm' }, [
      h(TopBar, {
        key: 't',
        activePersona,
        timeShift,
        onTimeShiftChange: setTimeShift,
        health,
      }),
      h(Messages, { key: 'msgs', messages, channel: activeChannel }),
      h(Composer, {
        key: 'c',
        disabled: !activePersonaId || !activeChannel,
        onSend,
      }),
    ]),
  ]);
}

const root = createRoot(document.getElementById('root'));
root.render(h(App));
