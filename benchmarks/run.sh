readonly RATES=(1000 10000 100000 1000000 2000000 4000000 8000000 10000000)
readonly WORKERS=(32)
readonly SECONDS=(300)
readonly BENCHMARKS="HiBench,Yahoo,NEXMark"
readonly DIR=$(dirname "$(readlink -f "$0")")

function status() {
    >&2 echo -e "\n\e[1m==> \e[33m$@\e[21m\n"
}

function eexit() {
    >&2 echo - "\n\e[1m!!> \e[41m$@\e[21m\n"
    exit 1
}

function run() {
    local rate="$1"
    local workers="$2"
    local seconds="$3"
    local benchmarks="$4"
    local output="$PWD"
    cd "$DIR"
    cargo run --release -- test \
	  --report "latencies" \
	  --threads "$workers" \
	  --seconds "$seconds" \
	  --events-per-second "$rate" \
          --benchmarks "$benchmarks" \
	  > "$output/$rate@$workers.csv" \
	|| eexit "Benchmark crashed."
}

function main() {
    local rate=("${RATES[@]}")
    local workers=("${WORKERS[@]}")
    local seconds=("${SECONDS[@]}")
    local benchmarks="$BENCHMARKS"
    
    while getopts "r:w:s:b:" opt; do
	case "$opt" in
	    r) rate=($OPTARG) ;;
	    w) workers=($OPTARG) ;;
	    s) seconds=($OPTARG) ;;
	    b) benchmarks=$OPTARG ;;
	    \?) >&2 echo "Invalid option: -$OPTARG"; exit 1;;
	esac
    done

    for r in "${rate[@]}"; do
	for w in "${workers[@]}"; do
	    for s in "${seconds[@]}"; do
		status "Running $w workers @ $r events/s for $s seconds."
		run "$r" "$w" "$s" "$benchmarks"
	    done
	done
    done
}

main "$@"
