// Command airlock runs control-plane replicas, region agents, operator
// commands, and the measurement suite.
package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Mohith26/airlock/internal/agent"
	"github.com/Mohith26/airlock/internal/bundle"
	"github.com/Mohith26/airlock/internal/cluster"
	"github.com/Mohith26/airlock/internal/raft"
	"github.com/Mohith26/airlock/internal/server"
)

const usage = `airlock: deployment control plane for disconnected regions

Cluster:
  airlock node     -id N -peers 1=http://h:7001,... -listen :7001 -data DIR -keys DIR
  airlock agent    -region R -control URLS -keys DIR -ledger FILE
  airlock agent    -region R -keys DIR -ledger FILE -import PKG -receipt OUT   (air-gapped, one shot)

Operator:
  airlock keygen   -out DIR
  airlock release  -control URLS -keys DIR -service S -version V -artifact FILE
  airlock deploy   -control URLS -job ID -region R -service S -version V
  airlock status   -control URLS
  airlock export   -control URLS -region R -out PKG
  airlock ack      -control URLS -receipt FILE

Measurement:
  airlock demo     -out results/demo.json
  airlock bench    -out results/bench.json [-jobs 10000 -seed 11]
  airlock failover -out results/failover.json [-trials 30]
`

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmds := map[string]func([]string) error{
		"node": cmdNode, "agent": cmdAgent, "keygen": cmdKeygen, "release": cmdRelease,
		"deploy": cmdDeploy, "status": cmdStatus, "export": cmdExport, "ack": cmdAck,
		"demo": cmdDemo, "bench": cmdBench, "failover": cmdFailover,
	}
	f, ok := cmds[os.Args[1]]
	if !ok {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err := f(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "airlock:", err)
		os.Exit(1)
	}
}

// ---- keys ----

func cmdKeygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	out := fs.String("out", "keys", "directory for key files")
	fs.Parse(args)
	if err := os.MkdirAll(*out, 0o700); err != nil {
		return err
	}
	for _, name := range []string{"release", "control"} {
		s, err := bundle.NewSigner(nil)
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*out, name+".key"), []byte(hex.EncodeToString(s.Priv.Seed())+"\n"), 0o600); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*out, name+".pub"), []byte(hex.EncodeToString(s.Pub)+"\n"), 0o644); err != nil {
			return err
		}
		fmt.Printf("%s key %s written to %s\n", name, s.ID, *out)
	}
	return nil
}

func loadSigner(path string) (*bundle.Signer, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	seed, err := hex.DecodeString(strings.TrimSpace(string(b)))
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("%s: not a hex ed25519 seed", path)
	}
	return bundle.NewSigner(seed)
}

func loadPub(path string) (ed25519.PublicKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pub, err := hex.DecodeString(strings.TrimSpace(string(b)))
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%s: not a hex ed25519 public key", path)
	}
	return pub, nil
}

// ---- cluster processes ----

func cmdNode(args []string) error {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	id := fs.Int("id", 1, "replica id")
	peers := fs.String("peers", "", "comma-separated id=url for every replica")
	listen := fs.String("listen", ":7001", "listen address")
	data := fs.String("data", "data", "data directory for the write-ahead log")
	keys := fs.String("keys", "keys", "directory with release.pub and control.key")
	fs.Parse(args)
	pm, err := server.ParsePeers(*peers)
	if err != nil {
		return err
	}
	if _, ok := pm[*id]; !ok {
		return fmt.Errorf("peers must include this node's id %d", *id)
	}
	relPub, err := loadPub(filepath.Join(*keys, "release.pub"))
	if err != nil {
		return err
	}
	cpKey, err := loadSigner(filepath.Join(*keys, "control.key"))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*data, 0o700); err != nil {
		return err
	}
	st, err := raft.OpenFileStorage(filepath.Join(*data, fmt.Sprintf("node-%d.wal", *id)), true)
	if err != nil {
		return err
	}
	s, err := server.New(server.Config{ID: *id, Peers: pm, Listen: *listen, Tick: 10 * time.Millisecond, Election: 15, Heartbeat: 3,
		Storage: st, ReleaseKeys: bundle.NewKeyring(relPub), PackageKey: cpKey, Seed: time.Now().UnixNano()})
	if err != nil {
		return err
	}
	if err := s.Start(nil); err != nil {
		return err
	}
	log.Printf("replica %d listening on %s", *id, *listen)
	wait()
	s.Stop()
	return st.Close()
}

func cmdAgent(args []string) error {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	region := fs.String("region", "", "region name")
	control := fs.String("control", "", "comma-separated control-plane URLs")
	keys := fs.String("keys", "keys", "directory with release.pub and control.pub")
	ledger := fs.String("ledger", "", "ledger file (defaults to <region>.ledger)")
	interval := fs.Duration("interval", time.Second, "sync interval")
	importPkg := fs.String("import", "", "air-gapped mode: import this offline package and exit")
	receipt := fs.String("receipt", "receipt.json", "where to write acknowledgements in -import mode")
	fs.Parse(args)
	if *region == "" {
		return fmt.Errorf("-region is required")
	}
	if *ledger == "" {
		*ledger = *region + ".ledger"
	}
	relPub, err := loadPub(filepath.Join(*keys, "release.pub"))
	if err != nil {
		return err
	}
	cpPub, err := loadPub(filepath.Join(*keys, "control.pub"))
	if err != nil {
		return err
	}
	l, err := agent.OpenFileLedger(*ledger)
	if err != nil {
		return err
	}
	defer l.Close()
	a := agent.New(agent.Config{Region: *region, ReleaseKeys: bundle.NewKeyring(relPub), PackageKeys: bundle.NewKeyring(cpPub), Ledger: l,
		OnEvent: func(e agent.Event) { log.Printf("%s %v", e.Type, e.Details) }})

	if *importPkg != "" {
		var p bundle.Package
		if err := readJSON(*importPkg, &p); err != nil {
			return err
		}
		acks, err := a.Import(p)
		if err != nil {
			return err
		}
		return writeJSON(*receipt, acks)
	}
	if *control == "" {
		return fmt.Errorf("-control is required unless -import is used")
	}
	cl := server.NewClient(strings.Split(*control, ",")...)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	t := time.NewTicker(*interval)
	defer t.Stop()
	log.Printf("agent for %s polling %s", *region, *control)
	for {
		if err := a.Sync(cl); err != nil {
			log.Printf("sync: %v (will retry)", err)
		}
		select {
		case <-stop:
			return nil
		case <-t.C:
		}
	}
}

func wait() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
}

// ---- operator commands ----

func controlFlag(fs *flag.FlagSet) *string {
	return fs.String("control", "http://127.0.0.1:7001,http://127.0.0.1:7002,http://127.0.0.1:7003", "comma-separated control-plane URLs")
}

func cmdRelease(args []string) error {
	fs := flag.NewFlagSet("release", flag.ExitOnError)
	control := controlFlag(fs)
	keys := fs.String("keys", "keys", "directory with release.key")
	service := fs.String("service", "", "service name")
	version := fs.String("version", "", "version")
	artifact := fs.String("artifact", "", "artifact file")
	fs.Parse(args)
	signer, err := loadSigner(filepath.Join(*keys, "release.key"))
	if err != nil {
		return err
	}
	art, err := os.ReadFile(*artifact)
	if err != nil {
		return err
	}
	m := signer.Sign(*service, *version, art, time.Now())
	r, err := server.NewClient(strings.Split(*control, ",")...).RegisterRelease(m, art)
	if err != nil {
		return err
	}
	fmt.Printf("registered %s@%s digest=%s duplicate=%v\n", *service, *version, m.Digest[:16], r.Duplicate)
	return nil
}

func cmdDeploy(args []string) error {
	fs := flag.NewFlagSet("deploy", flag.ExitOnError)
	control := controlFlag(fs)
	job := fs.String("job", "", "job id (idempotency key)")
	region := fs.String("region", "", "target region")
	service := fs.String("service", "", "service")
	version := fs.String("version", "", "version")
	fs.Parse(args)
	r, err := server.NewClient(strings.Split(*control, ",")...).Submit(*job, *region, *service, *version)
	if err != nil {
		return err
	}
	return printJSON(r)
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	control := controlFlag(fs)
	fs.Parse(args)
	st, err := server.NewClient(strings.Split(*control, ",")...).Status()
	if err != nil {
		return err
	}
	return printJSON(st)
}

func cmdExport(args []string) error {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	control := controlFlag(fs)
	region := fs.String("region", "", "air-gapped region")
	out := fs.String("out", "package.json", "output file")
	fs.Parse(args)
	p, err := server.NewClient(strings.Split(*control, ",")...).Export(*region)
	if err != nil {
		return err
	}
	fmt.Printf("package seq=%d with %d jobs written to %s\n", p.Sequence, len(p.Jobs), *out)
	return writeJSON(*out, p)
}

func cmdAck(args []string) error {
	fs := flag.NewFlagSet("ack", flag.ExitOnError)
	control := controlFlag(fs)
	receipt := fs.String("receipt", "receipt.json", "receipt produced by an air-gapped agent")
	fs.Parse(args)
	var acks []agent.Ack
	if err := readJSON(*receipt, &acks); err != nil {
		return err
	}
	cl := server.NewClient(strings.Split(*control, ",")...)
	for _, a := range acks {
		if err := cl.Ack(a.JobID, a.Status, a.Detail); err != nil {
			return err
		}
	}
	fmt.Printf("%d acknowledgements delivered\n", len(acks))
	return nil
}

// ---- measurement ----

func cmdDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	out := fs.String("out", "results/demo.json", "output file")
	seed := fs.Int64("seed", 7, "seed")
	fs.Parse(args)
	res, err := cluster.RunDemo(*seed)
	if err != nil {
		return err
	}
	for _, s := range res.Steps {
		fmt.Printf("%6d ms  %-10s %s %v\n", s.EndMS, s.ID, s.Title, s.Facts)
	}
	fmt.Printf("checks: %v\n", res.Checks)
	return writeJSON(*out, res)
}

func cmdBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	out := fs.String("out", "results/bench.json", "output file")
	seed := fs.Int64("seed", 11, "seed")
	jobs := fs.Int("jobs", 10000, "jobs")
	fs.Parse(args)
	cfg := cluster.DefaultBench(*seed)
	cfg.Jobs = *jobs
	res, err := cluster.RunBench(cfg)
	if err != nil {
		return err
	}
	fmt.Printf("%d jobs, %d leader kills, %d partitions, %d worker crashes, %d corruptions, %d tampered packages\n",
		res.Jobs.Unique, res.Faults.LeaderKills, res.Faults.Partitions, res.Faults.WorkerCrashes, res.Faults.CacheCorruptions, res.Faults.PackagesTampered)
	fmt.Printf("double executions %d, corrupt installs %d, pending %d, replicas identical %v, audit verified %v\n",
		res.Jobs.Double, res.Outcomes.CorruptInstalls, res.Jobs.Pending, res.ReplicasAgree, res.AuditVerified)
	return writeJSON(*out, res)
}

func cmdFailover(args []string) error {
	fs := flag.NewFlagSet("failover", flag.ExitOnError)
	out := fs.String("out", "results/failover.json", "output file")
	trials := fs.Int("trials", 30, "trials")
	fs.Parse(args)
	dir, err := os.MkdirTemp("", "airlock-failover-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	rep, err := server.MeasureFailover(*trials, dir)
	if err != nil {
		return err
	}
	fmt.Printf("%d trials: election p50 %.1f ms, p99 %.1f ms; writes back p50 %.1f ms, p99 %.1f ms; committed jobs lost %d\n",
		len(rep.Trials), rep.ElectionP50, rep.ElectionP99, rep.WriteGapP50, rep.WriteGapP99, rep.CommittedLost)
	return writeJSON(*out, rep)
}

// ---- helpers ----

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func writeJSON(path string, v any) error {
	if dir := filepath.Dir(path); dir != "." {
		os.MkdirAll(dir, 0o755)
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
