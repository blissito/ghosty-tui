//! ghosty-tui — chat mínimo en terminal contra un agente rust-ghosty.
//!
//! Uso:
//!   ghosty --agent <id> --token <embedToken> [--session <id>]
//!   (o env GHOSTY_AGENT / GHOSTY_TOKEN)
//!
//! agentId y embedToken salen del MCP de easybits: `agent_create({})`.

mod client;

use std::io::{self, Stdout};

use anyhow::{anyhow, Result};
use client::Frame;
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::StreamExt;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Terminal,
};
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// CLI

struct Cli {
    agent: String,
    token: String,
    session: String,
    once: Option<String>,
}

fn parse_args() -> Result<Cli> {
    let mut agent = std::env::var("GHOSTY_AGENT").ok();
    let mut token = std::env::var("GHOSTY_TOKEN").ok();
    let mut session = "default".to_string();
    let mut once = None;

    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--agent" => agent = it.next(),
            "--token" => token = it.next(),
            "--session" => {
                if let Some(s) = it.next() {
                    session = s;
                }
            }
            "--once" => once = it.next(),
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(anyhow!("argumento desconocido: {other}")),
        }
    }

    let agent = agent.filter(|s| !s.is_empty()).ok_or_else(|| {
        anyhow!("falta --agent <id> (o env GHOSTY_AGENT). Obtén uno con el MCP agent_create({{}}).")
    })?;
    let token = token.filter(|s| !s.is_empty()).ok_or_else(|| {
        anyhow!("falta --token <embedToken> (o env GHOSTY_TOKEN). Es el embedToken de agent_create.")
    })?;

    Ok(Cli { agent, token, session, once })
}

fn print_help() {
    println!(
        "ghosty-tui — chat en terminal con un agente rust-ghosty\n\n\
         USO:\n  \
           ghosty --agent <id> --token <embedToken> [--session <id>]\n  \
           ghosty --agent <id> --token <embedToken> --once \"mensaje\"   (headless, sin TUI)\n\n\
         ENV (fallback):\n  \
           GHOSTY_AGENT, GHOSTY_TOKEN\n\n\
         TECLAS:\n  \
           Enter  enviar    Esc / Ctrl+C  salir    PgUp/PgDn  scroll"
    );
}

// ---------------------------------------------------------------------------
// Estado

#[derive(Clone, Copy, PartialEq)]
enum Role {
    User,
    Bot,
    Error,
    System,
}

struct Msg {
    role: Role,
    text: String,
}

struct App {
    agent: String,
    session: String,
    messages: Vec<Msg>,
    input: String,
    streaming: bool,
    scroll_back: u16,
    stream_rx: Option<mpsc::Receiver<Frame>>,
}

impl App {
    fn new(agent: String, session: String) -> Self {
        let mut app = Self {
            agent: agent.clone(),
            session: session.clone(),
            messages: Vec::new(),
            input: String::new(),
            streaming: false,
            scroll_back: 0,
            stream_rx: None,
        };
        app.messages.push(Msg {
            role: Role::System,
            text: format!(
                "Conectado a {agent} · sesión {session}. Escribe y Enter para hablar. Esc para salir."
            ),
        });
        app
    }

    fn send(&mut self, text: String, client: reqwest::Client, token: String) {
        self.messages.push(Msg { role: Role::User, text: text.clone() });
        self.messages.push(Msg { role: Role::Bot, text: String::new() });
        self.streaming = true;
        self.scroll_back = 0;

        let (tx, rx) = mpsc::channel(64);
        self.stream_rx = Some(rx);
        let agent = self.agent.clone();
        let session = self.session.clone();
        tokio::spawn(async move {
            client::stream_message(client, agent, token, session, text, tx).await;
        });
    }

    fn on_frame(&mut self, frame: Frame) {
        match frame {
            Frame::Token(s) => {
                if let Some(m) = self.last_bot() {
                    m.text.push_str(&s);
                }
            }
            Frame::Error(e) => {
                // Si el turno falló antes de cualquier token, quita el bot vacío.
                if matches!(self.messages.last(), Some(m) if m.role == Role::Bot && m.text.is_empty())
                {
                    self.messages.pop();
                }
                self.messages.push(Msg { role: Role::Error, text: format!("⚠ {e}") });
                self.finish();
            }
            Frame::Done => self.finish(),
        }
    }

    fn finish(&mut self) {
        self.streaming = false;
        self.stream_rx = None;
    }

    fn last_bot(&mut self) -> Option<&mut Msg> {
        self.messages.iter_mut().rev().find(|m| m.role == Role::Bot)
    }
}

/// Espera el próximo frame, o pende para siempre si no hay stream activo.
/// Devolver `Frame::Done` al cerrarse el canal cierra el turno limpiamente.
async fn next_frame(rx: &mut Option<mpsc::Receiver<Frame>>) -> Frame {
    match rx {
        Some(r) => r.recv().await.unwrap_or(Frame::Done),
        None => std::future::pending().await,
    }
}

// ---------------------------------------------------------------------------
// Terminal (con guard para restaurar siempre, incluso en error/panic)

struct Tui {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl Tui {
    fn new() -> Result<Self> {
        enable_raw_mode()?;
        let mut out = io::stdout();
        execute!(out, EnterAlternateScreen)?;
        Ok(Self { terminal: Terminal::new(CrosstermBackend::new(out))? })
    }
}

impl Drop for Tui {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

// ---------------------------------------------------------------------------
// main / bucle

#[tokio::main]
async fn main() {
    let cli = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("ghosty-tui: {e}");
            std::process::exit(2);
        }
    };

    let client = reqwest::Client::new();

    // Modo headless: un turno, tokens a stdout, sin TUI. Útil para pipes y para verificar.
    if let Some(msg) = cli.once.clone() {
        std::process::exit(run_once(&client, &cli, &msg).await);
    }

    let mut app = App::new(cli.agent, cli.session);

    let result = {
        let mut tui = match Tui::new() {
            Ok(t) => t,
            Err(e) => {
                eprintln!("ghosty-tui: no pude iniciar la terminal: {e}");
                std::process::exit(1);
            }
        };
        run(&mut tui, &mut app, &client, &cli.token).await
        // `tui` se dropea aquí → terminal restaurada antes de imprimir nada.
    };

    if let Err(e) = result {
        eprintln!("ghosty-tui: {e}");
        std::process::exit(1);
    }
}

/// Un turno sin TUI: stremea tokens a stdout. Devuelve el código de salida.
async fn run_once(client: &reqwest::Client, cli: &Cli, msg: &str) -> i32 {
    use std::io::Write;
    let (tx, mut rx) = mpsc::channel(64);
    tokio::spawn(client::stream_message(
        client.clone(),
        cli.agent.clone(),
        cli.token.clone(),
        cli.session.clone(),
        msg.to_string(),
        tx,
    ));
    let mut code = 0;
    while let Some(frame) = rx.recv().await {
        match frame {
            Frame::Token(s) => {
                print!("{s}");
                let _ = io::stdout().flush();
            }
            Frame::Error(e) => {
                eprintln!("\nghosty-tui: error de stream: {e}");
                code = 1;
            }
            Frame::Done => break,
        }
    }
    println!();
    code
}

async fn run(tui: &mut Tui, app: &mut App, client: &reqwest::Client, token: &str) -> Result<()> {
    let mut events = EventStream::new();

    loop {
        tui.terminal.draw(|f| ui(f, app))?;

        tokio::select! {
            maybe = events.next() => {
                match maybe {
                    Some(Ok(Event::Key(k))) if k.kind == KeyEventKind::Press => {
                        match (k.code, k.modifiers) {
                            (KeyCode::Esc, _) => break,
                            (KeyCode::Char('c'), KeyModifiers::CONTROL) => break,
                            (KeyCode::Enter, _) => {
                                let text = app.input.trim().to_string();
                                if !text.is_empty() && !app.streaming {
                                    app.input.clear();
                                    app.send(text, client.clone(), token.to_string());
                                }
                            }
                            (KeyCode::Backspace, _) => { app.input.pop(); }
                            (KeyCode::PageUp, _) => {
                                app.scroll_back = app.scroll_back.saturating_add(5);
                            }
                            (KeyCode::PageDown, _) => {
                                app.scroll_back = app.scroll_back.saturating_sub(5);
                            }
                            (KeyCode::Char(c), m)
                                if m.is_empty() || m == KeyModifiers::SHIFT =>
                            {
                                app.input.push(c);
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(_)) => {}        // resize, mouse, etc. → re-render en el draw
                    Some(Err(_)) | None => break,
                }
            }
            frame = next_frame(&mut app.stream_rx) => {
                app.on_frame(frame);
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Render

fn ui(f: &mut ratatui::Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(1),    // historial
            Constraint::Length(1), // status
            Constraint::Length(3), // input (con borde)
        ])
        .split(f.area());

    // --- Historial ---
    let history_block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" ghosty · {} ", short(&app.agent)));
    let inner_w = chunks[0].width.saturating_sub(2) as usize;
    let inner_h = chunks[0].height.saturating_sub(2);

    let lines = build_lines(app, inner_w);
    let total = lines.len() as u16;
    let max_scroll = total.saturating_sub(inner_h);
    let scroll = max_scroll.saturating_sub(app.scroll_back);

    let history = Paragraph::new(lines).block(history_block).scroll((scroll, 0));
    f.render_widget(history, chunks[0]);

    // --- Status ---
    let status = if app.streaming {
        Span::styled(" ● ghosty está respondiendo…", Style::default().fg(Color::Yellow))
    } else {
        Span::styled(
            " ○ listo · Enter envía · Esc sale · PgUp/PgDn scroll",
            Style::default().fg(Color::DarkGray),
        )
    };
    f.render_widget(Paragraph::new(Line::from(status)), chunks[1]);

    // --- Input ---
    let input_block = Block::default().borders(Borders::ALL).title(" mensaje ");
    let input = Paragraph::new(app.input.as_str()).block(input_block);
    f.render_widget(input, chunks[2]);

    // Cursor dentro de la caja de input.
    let cx = chunks[2].x + 1 + (app.input.chars().count() as u16).min(chunks[2].width.saturating_sub(2));
    let cy = chunks[2].y + 1;
    f.set_cursor_position((cx, cy));
}

fn build_lines<'a>(app: &'a App, width: usize) -> Vec<Line<'a>> {
    let mut out = Vec::new();
    for msg in &app.messages {
        let (prefix, style) = match msg.role {
            Role::User => ("› ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Role::Bot => ("ghosty ", Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD)),
            Role::Error => ("", Style::default().fg(Color::Red)),
            Role::System => ("", Style::default().fg(Color::DarkGray).add_modifier(Modifier::ITALIC)),
        };
        let body_w = width.saturating_sub(prefix.chars().count()).max(1);
        let wrapped = wrap_text(&msg.text, body_w);
        for (i, chunk) in wrapped.iter().enumerate() {
            let body_style = if msg.role == Role::Bot {
                Style::default()
            } else {
                style
            };
            if i == 0 && !prefix.is_empty() {
                out.push(Line::from(vec![
                    Span::styled(prefix, style),
                    Span::styled(chunk.clone(), body_style),
                ]));
            } else {
                let indent = " ".repeat(prefix.chars().count());
                out.push(Line::from(Span::styled(format!("{indent}{chunk}"), body_style)));
            }
        }
        if wrapped.is_empty() {
            out.push(Line::from(""));
        }
    }
    out
}

/// Word-wrap simple por ancho de columnas (cuenta chars; suficiente para ES).
fn wrap_text(s: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return vec![s.to_string()];
    }
    let mut out = Vec::new();
    for raw in s.split('\n') {
        let mut cur = String::new();
        let mut cur_w = 0usize;
        for word in raw.split(' ') {
            let ww = word.chars().count();
            if ww > width {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
                let mut piece = String::new();
                let mut pw = 0;
                for ch in word.chars() {
                    if pw == width {
                        out.push(std::mem::take(&mut piece));
                        pw = 0;
                    }
                    piece.push(ch);
                    pw += 1;
                }
                cur = piece;
                cur_w = pw;
                continue;
            }
            let add = if cur.is_empty() { ww } else { ww + 1 };
            if cur_w + add > width {
                out.push(std::mem::take(&mut cur));
                cur.push_str(word);
                cur_w = ww;
            } else {
                if !cur.is_empty() {
                    cur.push(' ');
                    cur_w += 1;
                }
                cur.push_str(word);
                cur_w += ww;
            }
        }
        out.push(cur);
    }
    out
}

fn short(id: &str) -> String {
    if id.len() > 10 {
        format!("{}…", &id[..10])
    } else {
        id.to_string()
    }
}
