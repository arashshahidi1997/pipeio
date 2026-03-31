# pipeio flow navigator — source this from ~/.bashrc or ~/.zshrc:
#   source /path/to/pipeio/bin/pf.sh
#
# Requires: pipeio CLI on PATH (pip install -e packages/pipeio)

_PF_PIPEIO="${_PF_PIPEIO:-pipeio}"

# pf — navigate and operate on pipeio flows
#   pf                      list all flows
#   pf <flow>               cd into flow code directory
#   pf <flow> smk ...       run snakemake in flow context
#   pf <flow> path          print code directory path
#   pf <flow> config        print config path
#   pf <flow> deriv         cd into derivative directory
pf() {
  case "$1" in
    "")
      $_PF_PIPEIO flow list
      ;;
    -h|--help)
      cat >&2 <<'USAGE'
usage: pf                       list all flows
       pf <flow>                cd into flow code directory
       pf <flow> smk [args]     run snakemake in flow context
       pf <flow> path           print code directory path
       pf <flow> config         print config path
       pf <flow> deriv          cd into derivative directory
USAGE
      return 0
      ;;
    *)
      local flow="$1"
      shift
      case "${1:-}" in
        "")
          # cd into flow directory
          local p
          p="$($_PF_PIPEIO flow path "$flow" 2>/dev/null)"
          if [ -z "$p" ]; then
            echo "unknown flow: $flow" >&2
            return 1
          fi
          cd "$p"
          ;;
        smk)
          shift
          $_PF_PIPEIO flow smk "$flow" "$@"
          ;;
        path)
          $_PF_PIPEIO flow path "$flow"
          ;;
        config)
          $_PF_PIPEIO flow config "$flow"
          ;;
        deriv)
          local d
          d="$($_PF_PIPEIO flow deriv "$flow" 2>/dev/null)"
          if [ -z "$d" ]; then
            echo "no derivative directory for flow: $flow" >&2
            return 1
          fi
          cd "$d"
          ;;
        *)
          echo "unknown subcommand: $1 (try: smk, path, config, deriv)" >&2
          return 1
          ;;
      esac
      ;;
  esac
}

# Bash completion for pf
if [ -n "$BASH_VERSION" ]; then
  _pf() {
    local cur=${COMP_WORDS[COMP_CWORD]}
    local prev=${COMP_WORDS[1]}
    if [ "$COMP_CWORD" -eq 1 ]; then
      COMPREPLY=($(compgen -W "$($_PF_PIPEIO flow ids 2>/dev/null)" -- "$cur"))
    elif [ "$COMP_CWORD" -eq 2 ]; then
      COMPREPLY=($(compgen -W "smk path config deriv" -- "$cur"))
    fi
  }
  complete -F _pf pf
fi

# Zsh completion for pf
if [ -n "$ZSH_VERSION" ]; then
  _pf() {
    local subcmds="smk path config deriv"
    if (( CURRENT == 2 )); then
      compadd $($_PF_PIPEIO flow ids 2>/dev/null)
    elif (( CURRENT == 3 )); then
      compadd ${=subcmds}
    fi
  }
  compdef _pf pf
fi
