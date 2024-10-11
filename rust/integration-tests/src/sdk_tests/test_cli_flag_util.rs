
#[derive(Debug, Clone)]
pub struct TestArgs {
    pub diff: bool,
    pub output_path: Option<String>,
}

pub fn parse_test_args() -> TestArgs {
    // Capture the raw arguments
    let raw_args: Vec<String> = std::env::args().collect();

    // Log the raw arguments for debugging
    println!("Raw arguments: {:?}", raw_args);

    // Find the "--" separator (if it exists)
    let clap_args_position = raw_args.iter().position(|arg| arg == "--");

    // Only pass the arguments that come after "--", if it exists
    let custom_args: Vec<String> = match clap_args_position {
        Some(position) => raw_args[position + 1..].to_vec(), // Slice after `--`
        None => Vec::new(), // If no `--` is found, treat as no custom args
    };
    println!("Custom arguments: {:?}", custom_args);

    // Manually parse the "--diff" flag and the output path
    let diff_flag = custom_args.contains(&"--diff".to_string());
    let output_path = custom_args
        .iter()
        .position(|arg| arg == "--output_path")
        .and_then(|index| custom_args.get(index + 1).cloned());

    // Log the parsed values
    println!("Parsed diff_flag: {}", diff_flag);
    println!("Parsed output_path: {:?}", output_path);

    TestArgs {
        diff: diff_flag,
        output_path,
    }
}
