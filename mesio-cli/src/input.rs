use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    terminal,
};
use is_terminal::IsTerminal;
use pipeline_common::CancellationToken;
use std::io;
use std::time::Duration;
use tokio::io::AsyncBufReadExt;
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
    // Spawn a blocking task to read from stdin
    let handle = tokio::task::spawn_blocking(move || {
        let stdin = io::stdin();
        
        // Use a scope to ensure the lock is released properly
        loop {
            if token.is_cancelled() {
                break;
            }
            
            // Lock stdin for each read attempt to allow periodic cancellation checks
            let reader = stdin.lock();
            let mut lines = reader.lines();
            
            match lines.next() {
                Some(Ok(line)) => {
                    let trimmed = line.trim();
                    if trimmed == "q" {
                        println!("Cancellation requested. Shutting down gracefully...");
                        token.cancel();
                        break;
                    }
                }
                Some(Err(e)) => {
                    info!("Error reading from stdin: {}", e);
                    break;
                }
                None => {
                    // EOF reached
                    info!("EOF reached on stdin");
                    break;
                }
            }
            // Lock is automatically dropped here, allowing other operations
        }
    });
    
    // Add a timeout to prevent indefinite waiting
    let timeout_duration = Duration::from_millis(100);
    loop {
        if token.is_cancelled() {
            handle.abort();
            break;
        }
        
        match tokio::time::timeout(timeout_duration, &mut handle).await {
            Ok(_) => {
                // Task completed normally
                break;
            }
            Err(_) => {
                // Timeout occurred, check cancellation and continue
                continue;
            }
        }
    }
}
