'use client';

import React, { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

export default function Home() {
  const [query, setQuery] = useState('');
  const [isTyping, setIsTyping] = useState(false);
  const [messages, setMessages] = useState([
    { role: 'assistant', content: 'Hello! I am your Enterprise Data Assistant. I have full context of your dbt project and the live data layers. How can I help you today?' }
  ]);
  
  const [events, setEvents] = useState([
    { id: 1, type: 'CDC', msg: 'Debezium: 50 orders ingested', time: 'Just now' },
    { id: 2, type: 'DBT', msg: 'dbt: Materialized gold_revenue', time: '2m ago' },
    { id: 3, type: 'K8S', msg: 'Spark: Worker 02 healthy', time: '5m ago' },
  ]);

  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, isTyping]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;
    
    const userMsg = { role: 'user', content: query };
    setMessages(prev => [...prev, userMsg]);
    setQuery('');
    setIsTyping(true);
    
    try {
      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query })
      });
      const data = await response.json();
      setMessages(prev => [...prev, data]);
    } catch (error) {
      console.error('Error:', error);
      setMessages(prev => [...prev, { role: 'assistant', content: "I'm having trouble connecting to the data stack." }]);
    } finally {
      setIsTyping(false);
    }
  };

  return (
    <main className="container" style={{ maxWidth: '1400px', margin: '0 auto', padding: '2rem' }}>
      {/* Header */}
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '3rem' }}>
        <div>
          <h1 style={{ fontSize: '1.2rem', fontWeight: 600, color: '#fff' }}>Data Intelligence</h1>
          <p style={{ color: 'var(--muted)', fontSize: '0.75rem' }}>Analytical Control Center</p>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <div style={{ width: '6px', height: '6px', background: 'var(--success)', borderRadius: '50%' }}></div>
          <span style={{ fontSize: '0.75rem', color: 'var(--muted)', textTransform: 'uppercase' }}>Live</span>
        </div>
      </header>

      <div style={{ display: 'flex', gap: '4rem', height: '75vh' }}>
        
        {/* Main Chat Area (CLEAN & CENTERED) */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', height: '100%', background: 'rgba(255,255,255,0.02)', borderRadius: '24px', border: '1px solid var(--card-border)', overflow: 'hidden' }}>
          <div style={{ flex: 1, padding: '2.5rem', overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: '2rem' }}>
            {messages.map((msg, i) => (
              <div key={i} style={{ 
                alignSelf: msg.role === 'user' ? 'flex-end' : 'flex-start',
                maxWidth: msg.role === 'user' ? '80%' : '100%',
                display: 'flex',
                gap: '1.5rem'
              }}>
                {msg.role === 'assistant' && (
                  <div style={{ width: '32px', height: '32px', borderRadius: '8px', background: '#3b82f6', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: '0.8rem', flexShrink: 0 }}>AI</div>
                )}
                <div style={{ 
                  background: msg.role === 'user' ? 'rgba(59, 130, 246, 0.1)' : 'transparent',
                  padding: msg.role === 'user' ? '1rem 1.5rem' : '0',
                  borderRadius: '16px',
                  width: '100%',
                  lineHeight: 1.7,
                  fontSize: '1.05rem',
                  color: '#e2e8f0'
                }}>
                  <ReactMarkdown remarkPlugins={[remarkGfm]} components={{
                    table: ({node, ...props}) => <table style={{width: '100%', borderCollapse: 'collapse', margin: '1.5rem 0', border: '1px solid var(--card-border)'}} {...props} />,
                    th: ({node, ...props}) => <th style={{textAlign: 'left', padding: '0.75rem', background: 'rgba(255,255,255,0.03)', color: 'var(--muted)', fontSize: '0.8rem'}} {...props} />,
                    td: ({node, ...props}) => <td style={{padding: '0.75rem', borderBottom: '1px solid rgba(255,255,255,0.05)'}} {...props} />,
                    pre: ({node, ...props}) => <pre style={{background: '#0d1117', padding: '1.5rem', borderRadius: '12px', overflowX: 'auto', margin: '1.5rem 0'}} {...props} />,
                    code: ({node, ...props}) => <code style={{color: '#60a5fa'}} {...props} />,
                  }}>
                    {msg.content}
                  </ReactMarkdown>
                </div>
              </div>
            ))}
            {isTyping && <div style={{ color: 'var(--muted)', fontSize: '0.9rem' }}>Searching dbt models...</div>}
            <div ref={messagesEndRef} />
          </div>

          <div style={{ padding: '2rem', borderTop: '1px solid var(--card-border)', background: 'rgba(0,0,0,0.1)' }}>
            <form onSubmit={handleSubmit} style={{ position: 'relative' }}>
              <input 
                type="text" 
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Ask about revenue or orders..." 
                style={{ width: '100%', padding: '1.2rem 1.5rem', background: 'transparent', border: '1px solid var(--card-border)', borderRadius: '14px', color: 'white', outline: 'none' }} 
              />
              <button type="submit" style={{ position: 'absolute', right: '0.8rem', top: '50%', transform: 'translateY(-50%)', background: '#3b82f6', color: 'white', border: 'none', padding: '0.6rem 1.2rem', borderRadius: '10px', cursor: 'pointer' }}>Analyze</button>
            </form>
          </div>
        </div>

        {/* Side Panel (THE "MORE SHOWING" PART) */}
        <div style={{ width: '320px', display: 'flex', flexDirection: 'column', gap: '2rem' }}>
          
          <div className="glass" style={{ padding: '1.5rem' }}>
            <h3 style={{ fontSize: '0.75rem', textTransform: 'uppercase', color: 'var(--muted)', marginBottom: '1.5rem' }}>Pipeline Heartbeat</h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
              {events.map(e => (
                <div key={e.id} style={{ fontSize: '0.85rem' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', opacity: 0.6, fontSize: '0.7rem', marginBottom: '0.2rem' }}>
                    <span>{e.type}</span> <span>{e.time}</span>
                  </div>
                  <p>{e.msg}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="glass" style={{ padding: '1.5rem' }}>
            <h3 style={{ fontSize: '0.75rem', textTransform: 'uppercase', color: 'var(--muted)', marginBottom: '1.5rem' }}>Active Layers</h3>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
               <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.85rem' }}>
                 <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#3b82f6' }}></div>
                 <span>Gold Layer: **Verified**</span>
               </div>
               <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.85rem' }}>
                 <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#10b981' }}></div>
                 <span>Silver Layer: **Active**</span>
               </div>
               <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.85rem' }}>
                 <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#f59e0b' }}></div>
                 <span>Bronze Layer: **Streaming**</span>
               </div>
            </div>
          </div>

        </div>

      </div>
    </main>
  );
}
