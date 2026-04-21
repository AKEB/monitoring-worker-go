package buildinfo

// Version задаётся при сборке, например:
// go build -ldflags "-X monitoring-worker-go/internal/buildinfo.Version=v1.2.3" ...
// Если пусто, в конфиге подставится fallback (см. resolveWorkerVersion).
var Version = ""
