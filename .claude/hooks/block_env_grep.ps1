# PreToolUse Hook: Block Grep/Search operations targeting sensitive files/folders
# Claude Code passes tool input as JSON on stdin.
# Exit 2 = block the tool call with a reason message on stderr.
# Exit 0 = allow the tool call.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# Use absolute project root path (hooks live in <project>/.claude/hooks/)
$PROJECT_DIR = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path))

# --- Sensitive path patterns (regex, applied to normalized forward-slash paths) ---
$SENSITIVE_PATTERNS = @(
    '(^|/)environments(/|$)'        # environments/ folder
    '(^|/)\.env($|\.)'              # .env, .env.enc, .env.local, etc.
    '(^|/)\.git(/|$)'              # .git/ folder
    '(^|/)\.ssh(/|$)'             # .ssh/ folder
    '(^|/)\.gnupg(/|$)'           # .gnupg/ folder
    '(^|/)id_rsa($|\.)'           # SSH private keys
    '(^|/)id_ed25519($|\.)'       # SSH private keys (ed25519)
    '\.pem$'                       # PEM certificates/keys
    '\.key$'                       # Key files
    '\.pfx$'                       # PKCS#12 files
    '\.p12$'                       # PKCS#12 files
    '\.jks$'                       # Java keystores
    '\.keystore$'                  # Keystores
    '\.enc$'                       # Encrypted files (may contain secrets)
    '(^|/)secrets\.ya?ml$'         # secrets.yaml / secrets.yml
    '(^|/)credentials($|\.)'       # credentials files
    '(^|/)\.aws(/|$)'             # AWS config/credentials
    '(^|/)\.kube(/|$)'            # Kubernetes config
    '(^|/)vault\.ya?ml$'          # Vault config
    '(^|/)\.docker(/|$)'          # Docker config (may have registry creds)
)

# --- Sensitive query patterns (block searches for secrets/keys in content) ---
$SENSITIVE_QUERY_PATTERNS = @(
    '(?i)(password|passwd|secret.?key|api.?key|access.?key|private.?key|token)\s*[:=]'
    '(?i)(aws_secret|aws_access|ENCRYPT_KEY|ENCRYPT_IV)'
    '(?i)BEGIN\s+(RSA|DSA|EC|OPENSSH)\s+PRIVATE\s+KEY'
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

# --- Collect path fields to check ---
$paths_to_check = @()

if ($tool_input.includePattern)  { $paths_to_check += [string]$tool_input.includePattern }
if ($tool_input.filePath)        { $paths_to_check += [string]$tool_input.filePath }
if ($tool_input.file_path)       { $paths_to_check += [string]$tool_input.file_path }
if ($tool_input.path)            { $paths_to_check += [string]$tool_input.path }
if ($tool_input.directory)       { $paths_to_check += [string]$tool_input.directory }

# --- Check paths for sensitive locations ---
foreach ($p in $paths_to_check) {
    if ([string]::IsNullOrWhiteSpace($p)) { continue }

    $normalized = "$p" -replace '\\', '/'

    # Block path traversal
    if ($normalized -match '(^|/)\.\.(/|$)') {
        [Console]::Error.WriteLine("BLOCKED: Path traversal ('..') detected in grep path: $p. This is not allowed for security reasons.")
        exit 2
    }

    # Check against sensitive path patterns
    foreach ($pattern in $SENSITIVE_PATTERNS) {
        if ($normalized -match $pattern) {
            [Console]::Error.WriteLine("BLOCKED: Grep/Search access to sensitive path is not allowed. Matched pattern '$pattern' in: $p. Sensitive files must not be searched.")
            exit 2
        }
    }
}

# --- Check search query for secret-hunting patterns ---
$search_query = $null
if ($tool_input.query) { $search_query = [string]$tool_input.query }
if ($tool_input.pattern) { $search_query = [string]$tool_input.pattern }

if ($search_query) {
    foreach ($qp in $SENSITIVE_QUERY_PATTERNS) {
        if ($search_query -match $qp) {
            [Console]::Error.WriteLine("BLOCKED: Search query appears to be hunting for secrets/credentials. Pattern matched: '$qp'. This is not allowed.")
            exit 2
        }
    }
}

# All clear
exit 0
