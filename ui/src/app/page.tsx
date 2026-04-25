'use client';

import React, { useState } from 'react';

export default function Home() {
  const [query, setQuery] = useState('');
  const [messages, setMessages] = useState([
    { role: 'assistant', content: 'Hello! I am your Data Assistant. I have full context of the SuperShoes database and your dbt project. How can I help you today?' }
  ]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;
    
    const userMsg = { role: 'user', content: query };
    setMessages(prev => [...prev, userMsg]);
    setQuery('');
    
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
      setMessages(prev => [...prev, { role: 'assistant', content: "Sorry, I'm having trouble connecting to the AI Brain right now." }]);
    }
  };

  return (
    <main className="container">
      {/* Header */}
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '3rem' }}>
        <div>
          <h1 style={{ fontSize: '1.5rem', fontWeight: 700, background: 'linear-gradient(to right, #3b82f6, #8b5cf6)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
            Modern ETL Stack
          </h1>
          <p style={{ color: 'var(--muted)', fontSize: '0.875rem' }}>Autonomous Data Platform</p>
        </div>
        <div className="glass" style={{ padding: '0.5rem 1rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <div style={{ width: '8px', height: '8px', background: 'var(--success)', borderRadius: '50%', boxShadow: '0 0 10px var(--success)' }}></div>
          <span style={{ fontSize: '0.875rem', fontWeight: 500 }}>Cluster Healthy</span>
        </div>
      </header>

      {/* Stats Grid */}
      <section className="grid grid-cols-3" style={{ marginBottom: '3rem' }}>
        <div className="glass" style={{ padding: '1.5rem' }}>
          <p style={{ color: 'var(--muted)', fontSize: '0.875rem', marginBottom: '0.5rem' }}>CDC Throughput</p>
          <h2 style={{ fontSize: '1.5rem', fontWeight: 700 }}>1.2k <span style={{ fontSize: '1rem', color: 'var(--success)' }}>req/s</span></h2>
        </div>
        <div className="glass" style={{ padding: '1.5rem' }}>
          <p style={{ color: 'var(--muted)', fontSize: '0.875rem', marginBottom: '0.5rem' }}>Storage Used (MinIO)</p>
          <h2 style={{ fontSize: '1.5rem', fontWeight: 700 }}>45.2 <span style={{ fontSize: '1rem', color: 'var(--primary)' }}>GB</span></h2>
        </div>
        <div className="glass" style={{ padding: '1.5rem' }}>
          <p style={{ color: 'var(--muted)', fontSize: '0.875rem', marginBottom: '0.5rem' }}>Data Freshness</p>
          <h2 style={{ fontSize: '1.5rem', fontWeight: 700 }}>&lt; 2s <span style={{ fontSize: '1rem', color: 'var(--accent)' }}>latency</span></h2>
        </div>
      </section>

      {/* Chat Area */}
      <section className="glass" style={{ height: '500px', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
        <div style={{ flex: 1, padding: '2rem', overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
          {messages.map((msg, i) => (
            <div key={i} style={{ 
              alignSelf: msg.role === 'user' ? 'flex-end' : 'flex-start',
              maxWidth: '80%',
              padding: '1rem 1.5rem',
              borderRadius: '12px',
              background: msg.role === 'user' ? 'var(--primary)' : 'rgba(255,255,255,0.05)',
              fontSize: '0.95rem',
              lineHeight: 1.5
            }}>
              {msg.content}
            </div>
          ))}
        </div>

        {/* Input Bar */}
        <div style={{ padding: '1.5rem', borderTop: '1px solid var(--card-border)' }}>
          <form onSubmit={handleSubmit} style={{ position: 'relative' }}>
            <input 
              type="text" 
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Ask anything about your data..." 
              style={{ 
                width: '100%', 
                padding: '1.2rem 1.5rem', 
                background: 'rgba(255,255,255,0.03)', 
                border: '1px solid var(--card-border)', 
                borderRadius: '12px',
                color: 'white',
                fontSize: '1rem',
                outline: 'none'
              }} 
            />
            <button type="submit" style={{ 
              position: 'absolute', 
              right: '0.8rem', 
              top: '50%', 
              transform: 'translateY(-50%)',
              background: 'var(--primary)',
              color: 'white',
              border: 'none',
              padding: '0.6rem 1.2rem',
              borderRadius: '8px',
              fontWeight: 600,
              cursor: 'pointer'
            }}>
              Send
            </button>
          </form>
        </div>
      </section>

      <footer style={{ marginTop: '3rem', textAlign: 'center', color: 'var(--muted)', fontSize: '0.875rem' }}>
        &copy; 2026 Modern ETL Stack | Powered by dbt MCP
      </footer>
    </main>
  );
}
