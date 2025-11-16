use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    terminal,
};
use is_terminal::IsTerminal;
use pipeline_common::CancellationToken;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::info;

/// Asynchronously listens for user input to trigger cancellation.
///
/// This function checks if stdin is a terminal or piped input:
/// - If stdin is a terminal, it enables raw mode and listens for key presses.
///   Pressing 'q' or Ctrl+C will trigger the cancellation token.
/// - If stdin is not a terminal (piped input), it reads lines from stdin.
///   If a line contains 'q', it triggers the cancellation token.
///
/// The function will clean up by disabling raw mode when the token is cancelled.
pub async fn input_handler(token: CancellationToken) {
    let stdin = io::stdin();
    
    // Check if stdin is a terminal
    if stdin.is_terminal() {
        // Terminal mode: use keyboard events
        handle_terminal_input(token).await;
    } else {
        // Stdin mode: read from piped input
        handle_stdin_input(token).await;
    }
}

/// Handle input from terminal keyboard events
async fn handle_terminal_input(token: CancellationToken) {
    if terminal::enable_raw_mode().is_err() {
        info!("Failed to enable raw mode. Input handling will be disabled.");
        return;
    }

    loop {
        // Check for cancellation signal first.
        if token.is_cancelled() {
            break;
        }

        // Poll for keyboard events with a timeout.
        if let Ok(true) = event::poll(Duration::from_millis(100))
            && let Ok(Event::Key(key_event)) = event::read()
        {
            let should_cancel = match key_event {
                // Handle 'q' key press
                KeyEvent {
                    code: KeyCode::Char('q'),
                    modifiers: KeyModifiers::NONE,
                    ..
                } => true,
                // Handle Ctrl+C
                KeyEvent {
                    code: KeyCode::Char('c'),
                    modifiers: KeyModifiers::CONTROL,
                    ..
                } => true,
                _ => false,
            };

            if should_cancel {
                println!("Cancellation requested. Shutting down gracefully...");
                token.cancel();
                break;
            }
        }
    }

    if terminal::disable_raw_mode().is_err() {
        info!("Failed to disable raw mode.");
    }
}

/// Handle input from stdin (piped input)
async fn handle_stdin_input(token: CancellationToken) {
    // Use tokio's async stdin
    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    
    loop {
        tokio::select! {
            // Check for cancellation first (biased to prioritize cancellation)
            biased;
            
            _ = token.cancelled() => {
                info!("Stdin handler cancelled");
                break;
            }
            
            // Try to read the next line
            result = lines.next_line() => {
                match result {
                    Ok(Some(line)) => {
                        let trimmed = line.trim();
                        if trimmed == "q" {
                            println!("Cancellation requested. Shutting down gracefully...");
                            token.cancel();
                            break;
                        }
                    }
                    Ok(None) => {
                        // EOF reached
                        info!("Stdin EOF reached");
                        break;
                    }
                    Err(e) => {
                        info!("Error reading from stdin: {}", e);
                        break;
                    }
                }
            }
        }
    }
}
