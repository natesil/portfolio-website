import { useState } from 'react'
import './App.css'

function App() {
  const [showChatbot, setShowChatbot] = useState(false)

  return (
    <div className="portfolio">
      <nav className="navbar">
        <div className="nav-left">
          <a href="#" className="nav-link">My Portfolio</a>
        </div>
        <div className="nav-right">
          <a href="#about" className="nav-link">About</a>
          <a href="#projects" className="nav-link">Projects</a>
          <a href="#skills" className="nav-link">Skills</a>
          <a href="#contact" className="nav-link">Contact</a>
        </div>
      </nav>

      <div className="hero">
        <div className="hero-content">
          <h1 className="hero-title">Nathan Silverman</h1>
          <p className="hero-subtitle">Data & ML Engineer | Skier | Athlete</p>
          <a href="#projects" className="cta-button">View My Work</a>
        </div>
      </div>

      <section id="about" className="section">
        <div className="section-container">
          <h2 className="section-title">About</h2>
          <p className="section-text">
            Add your bio and background here. Tell your story as a Data & ML Engineer,
            your passion for skiing and athletics, and what drives you in your work.
          </p>
        </div>
      </section>

      <section id="projects" className="section section-alt">
        <div className="section-container">
          <h2 className="section-title">Projects</h2>
          <div className="projects-grid">
            <div className="project-card">
              <h3 className="project-title">Ski Conditions Intelligence</h3>
              <p className="project-description">Natural language queries on historical weather data using Data Vault modeling and text-to-SQL.</p>
              <button className="project-demo-btn" onClick={() => setShowChatbot(true)}>
                Try Demo
              </button>
            </div>
            <div className="project-card">
              <h3 className="project-title">Project 2</h3>
              <p className="project-description">Description of your second project.</p>
            </div>
            <div className="project-card">
              <h3 className="project-title">Project 3</h3>
              <p className="project-description">Description of your third project.</p>
            </div>
          </div>
        </div>
      </section>

      <section id="skills" className="section">
        <div className="section-container">
          <h2 className="section-title">Skills</h2>
          <div className="skills-grid">
            <div className="skill-category">
              <h3 className="skill-category-title">Languages</h3>
              <ul className="skill-list">
                <li>Python</li>
                <li>JavaScript</li>
                <li>SQL</li>
              </ul>
            </div>
            <div className="skill-category">
              <h3 className="skill-category-title">ML/Data</h3>
              <ul className="skill-list">
                <li>TensorFlow</li>
                <li>PyTorch</li>
                <li>Pandas</li>
              </ul>
            </div>
            <div className="skill-category">
              <h3 className="skill-category-title">Tools</h3>
              <ul className="skill-list">
                <li>Git</li>
                <li>Docker</li>
                <li>AWS</li>
              </ul>
            </div>
          </div>
        </div>
      </section>

      <section id="contact" className="section section-alt">
        <div className="section-container">
          <h2 className="section-title">Contact</h2>
          <p className="section-text">Get in touch with me!</p>
          <div className="contact-links">
            <a href="mailto:your.email@example.com" className="contact-link">Email</a>
            <a href="https://linkedin.com/in/yourprofile" className="contact-link">LinkedIn</a>
            <a href="https://github.com/yourusername" className="contact-link">GitHub</a>
          </div>
        </div>
      </section>

      {showChatbot && (
        <div className="modal-overlay" onClick={() => setShowChatbot(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h2>Ski Conditions Chatbot</h2>
              <button className="modal-close" onClick={() => setShowChatbot(false)}>×</button>
            </div>
            <div className="chat-container">
              <div className="chat-messages">
                <div className="chat-message bot">
                  <p>Ask me about historical ski conditions! For example:</p>
                  <ul>
                    <li>"Which weekends had the most snowfall at Sunday River?"</li>
                    <li>"What was the average snowfall in January 2023?"</li>
                    <li>"Show me powder days at Stowe"</li>
                  </ul>
                  <p className="note">Note: Backend not yet connected. Coming soon!</p>
                </div>
              </div>
              <div className="chat-input-container">
                <input
                  type="text"
                  className="chat-input"
                  placeholder="Ask about ski conditions..."
                  disabled
                />
                <button className="chat-send-btn" disabled>Send</button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default App
