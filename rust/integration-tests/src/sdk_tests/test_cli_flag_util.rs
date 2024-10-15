#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TestArgs {
    pub generate_output: bool,
    pub output_path: Option<String>,
}

#[allow(dead_code)]
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

    // Manually parse the "--generate-output" flag
    let generate_output_flag = custom_args.contains(&"--generate-output".to_string());

    // Manually parse the "--output-path" flag and get its associated value
    let output_path = custom_args
        .windows(2)
        .find(|args| args[0] == "--output-path")
        .map(|args| args[1].clone()); // Correct the flag name to `--output-path`

    // Log the parsed values
    println!("Parsed generate_output_flag: {}", generate_output_flag);
    println!("Parsed output_path: {:?}", output_path);

    TestArgs {
        generate_output: generate_output_flag,
        output_path,
    }
}
