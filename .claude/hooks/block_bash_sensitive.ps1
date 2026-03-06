# PreToolUse Hook: Block shell commands that access sensitive files/folders
# Claude Code passes tool input as JSON on stdin.
# Exit 2 = block the tool call with a reason message on stderr.
# Exit 0 = allow the tool call.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Use absolute project root path (hooks live in <project>/.claude/hooks/)
$PROJECT_DIR = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))

# --- Sensitive path patterns (matched against the full command string) ---
$SENSITIVE_PATH_PATTERNS = @(
    'environments[/\\]'
    'environments\s'
    'environments$'
    '\.env($|\.|\s)'
    '\.env\.enc'
    '\.env\.local'
    '\.git[/\\]'
    '\.ssh[/\\]'
    '\.gnupg[/\\]'
    'id_rsa'
    'id_ed25519'
    '\.pem(\s|$)'
    '\.key(\s|$)'
    '\.pfx(\s|$)'
    '\.p12(\s|$)'
    '\.jks(\s|$)'
    '\.keystore(\s|$)'
    '\.enc(\s|$)'
    'secrets\.ya?ml'
    '\.aws[/\\]'
    '\.kube[/\\]'
    '\.docker[/\\]'
)

# --- Dangerous command patterns ---
$DANGEROUS_COMMAND_PATTERNS = @(
    '(?i)curl\s+.*upload.*environments'
    '(?i)(curl|wget|nc|ncat)\s+.*\.(env|pem|key|enc)'
    '(?i)(base64|xxd)\s+.*\.(env|pem|key|enc)'
    '(?i)scp\s+.*environments'
    '(?i)printenv'
    '(?i)set\s*$'                    # bare 'set' dumps env vars in some shells
)

# --- Read and parse JSON input from stdin ---
$input_json = $Input | Out-String
if ([string]::IsNullOrWhiteSpace($input_json)) {
    exit 0
}

try {
    $data = $input_json | ConvertFrom-Json
} catch {
    exit 0
}

$tool_input = $data.tool_input
if (-not $tool_input) {
    exit 0
}

# --- Get the command string ---
$command = $null
if ($tool_input.command) { $command = [string]$tool_input.command }
if (-not $command) {
    # No command to check
    exit 0
}

# Normalize backslashes to forward slashes for matching
$normalized_cmd = "$command" -replace '\\', '/'

# --- Block path traversal in commands ---
if ($normalized_cmd -match '\.\./.*environments' -or
    $normalized_cmd -match '\.\./.*\.env' -or
    $normalized_cmd -match '\.\./.*\.(pem|key|enc|pfx|p12|jks)') {
    [Console]::Error.WriteLine("BLOCKED: Path traversal to sensitive files detected in command. This is not allowed.")
    exit 2
}

# General path traversal that escapes the project
if ($normalized_cmd -match '(^|\s)\.\./\.\.' ) {
    [Console]::Error.WriteLine("BLOCKED: Deep path traversal ('../../') detected in command. This is not allowed for security reasons.")
    exit 2
}

# --- Check for commands that read sensitive files ---
# Build list of file-reading commands to inspect
$READ_COMMANDS = @(
    'cat', 'less', 'more', 'head', 'tail', 'type',
    'Get-Content', 'gc', 'Select-String',
    'vi', 'vim', 'nano', 'notepad', 'code',
    'cp', 'copy', 'move', 'mv', 'ren', 'rename',
    'tar', 'zip', '7z', 'Compress-Archive',
    'scp', 'rsync'
)

foreach ($pattern in $SENSITIVE_PATH_PATTERNS) {
    if ($normalized_cmd -match $pattern) {
        [Console]::Error.WriteLine("BLOCKED: Shell command references a sensitive path (matched: '$pattern'). Access to environments/, .env, .git/, keys, and credentials via shell is not allowed.")
        exit 2
    }
}

# --- Check for dangerous command patterns ---
foreach ($dcp in $DANGEROUS_COMMAND_PATTERNS) {
    if ($normalized_cmd -match $dcp) {
        [Console]::Error.WriteLine("BLOCKED: Potentially dangerous command pattern detected (matched: '$dcp'). This operation is not allowed for security reasons.")
        exit 2
    }
}

# All clear
exit 0

