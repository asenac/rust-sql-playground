use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, Result};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

fn main() -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    loop {
        let readline = rl.readline("SQL> ");
        match readline {
            Ok(line) => {
                println!("Line: {}", line);
                match Parser::parse_sql(&dialect, &line) {
                    Ok(ast) => println!("AST: {:?}", ast),
                    Err(err) => println!("Error: {}", err),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}
