// Package project reads the instructions and skills a workspace gives a run.
// Ephemeral runs resolve paths inside one os.Root opened on the workspace
// directory. Persistent runs use the same package inside canti-job, where
// ordinary kernel resolution sees exactly the job's mount view.
package project

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"unicode/utf8"
)

// The bounds of a project snapshot. Instruction content is refused rather
// than cut; the skills index is cut because it is an index, not an
// instruction.
const (
	MaxInstructionBytes = 64 << 10
	MaxInstructionFiles = 64
	MaxSkillsIndexBytes = 16 << 10
	MaxIndexFooterBytes = 512
	MaxSkillFileBytes   = 64 << 10
	MaxSkillNameLength  = 64
	MaxSkillDescription = 1024
	MaxSnapshotSkills   = 64
	MaxSnapshotSkipped  = 64
	MaxSkipReasonBytes  = 512
	// MaxSnapshotEncodedBytes bounds the helper's JSON result after escaping.
	// Required instruction content plus its paths and the rendered index fit;
	// optional skill and diagnostic pages are dropped if needed.
	MaxSnapshotEncodedBytes = 4 << 20
	// MaxDiagnosticBytes bounds a diagnostic assembled from workspace or
	// metadata values before any view or status forwards it.
	MaxDiagnosticBytes = 256 << 10
)

// InstructionNames is Pi's per-directory precedence. Pi 1.5.0
// dist/core/resource-loader.js loadContextFileFromDir tries these names in
// this order and loads at most one file per directory; a later name is
// considered only when the earlier one is absent or not a regular file.
var InstructionNames = []string{"AGENTS.override.md", "AGENTS.md", "AGENTS.MD", "CLAUDE.md", "CLAUDE.MD"}

var (
	// ErrUnsafePath reports a path that leaves its workspace or is not a
	// directory where one is required.
	ErrUnsafePath = errors.New("project: path leaves the workspace")
	// ErrNotFound reports a path that does not exist.
	ErrNotFound = errors.New("project: path not found")
	// ErrDangling reports a symbolic link whose immediate target does not exist.
	ErrDangling = errors.New("project: dangling symbolic link")
	// ErrForbidden reports a resolved target named like an environment file.
	ErrForbidden = errors.New("project: environment files are not readable")
	// ErrUnreadableMount reports a path under a volume, tmpfs, or other mount
	// without a readable bind source.
	ErrUnreadableMount = errors.New("project: unreadable mount")
)

// Request names the host workspace and the paths a job sees for an ephemeral
// run. Root and Workdir are container-visible paths; Workspace is their host
// directory.
type Request struct {
	Workspace string
	Root      string
	Workdir   string
}

// Snapshot is the durable project context of one run.
type Snapshot struct {
	Root              string        `json:"root,omitempty"`
	Instructions      []Instruction `json:"instructions,omitempty"`
	Skills            []Skill       `json:"skills,omitempty"`
	SkillsTotal       int           `json:"skills_total,omitempty"`
	Skipped           []Skip        `json:"skipped,omitempty"`
	SkippedTotal      int           `json:"skipped_total,omitempty"`
	SkillsIndex       string        `json:"skills_index,omitempty"`
	SkillsIndexSHA256 string        `json:"skills_index_sha256,omitempty"`
	SkillsOmitted     int           `json:"skills_omitted,omitempty"`
}

// Meta is the content-free snapshot projection stored beside the run and read
// by status, acknowledgements, progress and resume.
type Meta struct {
	Root              string     `json:"root,omitempty"`
	Instructions      []MetaFile `json:"instructions,omitempty"`
	Skills            []Skill    `json:"skills,omitempty"`
	SkillsTotal       int        `json:"skills_total,omitempty"`
	Skipped           []Skip     `json:"skipped,omitempty"`
	SkippedTotal      int        `json:"skipped_total,omitempty"`
	SkillsIndexBytes  int        `json:"skills_index_bytes,omitempty"`
	SkillsIndexSHA256 string     `json:"skills_index_sha256,omitempty"`
	SkillsOmitted     int        `json:"skills_omitted,omitempty"`
	InstructionBytes  int        `json:"instruction_bytes,omitempty"`
}

// MetaFile is one instruction file's metadata.
type MetaFile struct {
	Path         string `json:"path"`
	ResolvedPath string `json:"resolved_path,omitempty"`
	Size         int    `json:"size"`
	SHA256       string `json:"sha256"`
}

// Instruction is one instruction file. Path is the name the job reads and
// ResolvedPath is the regular file it resolves to; they differ when the name
// or a parent directory is a symbolic link.
type Instruction struct {
	Path         string `json:"path"`
	ResolvedPath string `json:"resolved_path,omitempty"`
	Size         int    `json:"size"`
	SHA256       string `json:"sha256"`
	Content      string `json:"content"`
}

// Skill is one valid, model-invocable skill in the index.
type Skill struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	Path         string `json:"path"`
	ResolvedPath string `json:"resolved_path,omitempty"`
}

// Skip is one skill-like path that did not enter the index, with the reason.
type Skip struct {
	Path   string `json:"path,omitempty"`
	Reason string `json:"reason"`
}

// Read binds an ephemeral run's instructions and skills from the host
// workspace. Relative links are followed inside the workspace; an absolute
// link target is not followed because the host cannot resolve it as the job's
// container would.
func Read(req Request) (Snapshot, error) {
	if err := req.validate(); err != nil {
		return Snapshot{}, err
	}
	root, err := os.OpenRoot(req.Workspace)
	if err != nil {
		return Snapshot{}, fmt.Errorf("project: open workspace: %w", err)
	}
	defer root.Close()
	chain, err := chainOf(root, req.Workspace, req.Root, req.Workdir)
	if err != nil {
		return Snapshot{}, err
	}
	s := Snapshot{Root: req.Root}
	if s.Instructions, err = instructionsIn(root, req.Workspace, req.Root, chain); err != nil {
		return Snapshot{}, err
	}
	skills, skipped := skillsIn(root, req.Workspace, req.Root, chain)
	s.SkillsTotal, s.SkippedTotal = len(skills), len(skipped)
	s.Skills = firstSkills(skills)
	s.Skipped = firstSkips(skipped)
	if len(skills) > 0 {
		index := buildIndex(skills, len(skipped))
		s.SkillsIndex = index.Encode()
		s.SkillsOmitted = index.Omitted
		s.SkillsIndexSHA256 = digest([]byte(index.Text()))
	}
	if len(s.Instructions) == 0 && len(s.Skills) == 0 && len(s.Skipped) == 0 {
		s.Root = ""
	}
	return s, s.Validate()
}

func (r Request) validate() error {
	if r.Workspace == "" || !filepath.IsAbs(r.Workspace) || filepath.Clean(r.Workspace) != r.Workspace {
		return errors.New("project: workspace must be an absolute clean directory")
	}
	if r.Root == "" || !path.IsAbs(r.Root) || path.Clean(r.Root) != r.Root {
		return errors.New("project: root must be an absolute clean path")
	}
	if r.Workdir == "" || !path.IsAbs(r.Workdir) || path.Clean(r.Workdir) != r.Workdir || !visibleWithin(r.Root, r.Workdir) {
		return errors.New("project: working directory must be inside the root")
	}
	return nil
}

type chainDir struct {
	visible  string
	rel      string
	resolved string
}

func chainOf(root *os.Root, workspace, visibleRoot, visibleWorkdir string) ([]chainDir, error) {
	rel := strings.TrimPrefix(visibleWorkdir, visibleRoot)
	rel = strings.TrimPrefix(rel, "/")
	out := make([]chainDir, 0, 1)
	seen := map[string]bool{}
	add := func(visible, resolved string) {
		if seen[resolved] {
			return
		}
		seen[resolved] = true
		out = append(out, chainDir{visible: visible, rel: resolved})
	}
	rootResolved, info, _, err := resolvePath(root, ".", 0, false)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: %s is not a directory", ErrUnsafePath, visibleRoot)
	}
	if err := checkDir(root, workspace, rootResolved); err != nil {
		return nil, err
	}
	add(visibleRoot, rootResolved)
	if rel == "" {
		return out, nil
	}
	prefix := ""
	for _, component := range strings.Split(rel, "/") {
		if component == "" || component == "." {
			continue
		}
		prefix = path.Join(prefix, component)
		visible := path.Join(visibleRoot, prefix)
		resolved, info, _, err := resolvePath(root, prefix, 0, false)
		if errors.Is(err, ErrNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%w: %s is not a directory", ErrUnsafePath, visible)
		}
		if err := checkDir(root, workspace, resolved); err != nil {
			return nil, err
		}
		add(visible, resolved)
	}
	return out, nil
}

func instructionsIn(root *os.Root, workspace, visibleRoot string, chain []chainDir) ([]Instruction, error) {
	var found []instructionFile
	for _, dir := range chain {
		for _, name := range InstructionNames {
			file, ok, err := instruction(root, visibleRoot, dir, name)
			if err != nil {
				return nil, err
			}
			if ok {
				found = append(found, file)
				break
			}
		}
	}
	if len(found) > MaxInstructionFiles {
		return nil, fmt.Errorf("project: %d instruction files, over the %d-file limit", len(found), MaxInstructionFiles)
	}
	var total int64
	for _, file := range found {
		total += file.size
	}
	if total > MaxInstructionBytes {
		return nil, instructionBoundError(found, total)
	}
	out := make([]Instruction, 0, len(found))
	for _, file := range found {
		content, real, err := readExact(root, workspace, file.rel, file.size)
		if err != nil {
			return nil, fmt.Errorf("project: read instruction file %s: %w", file.path, err)
		}
		if !utf8.Valid(content) {
			return nil, fmt.Errorf("project: instruction file %s is not valid UTF-8", file.path)
		}
		resolved, err := visibleHostPath(workspace, visibleRoot, real)
		if err != nil {
			return nil, fmt.Errorf("project: instruction file %s: %w", file.path, err)
		}
		out = append(out, Instruction{Path: file.path, ResolvedPath: differ(file.path, resolved), Size: len(content), SHA256: digest(content), Content: string(content)})
	}
	return out, nil
}

type instructionFile struct {
	path     string
	resolved string
	rel      string
	size     int64
}

func instruction(root *os.Root, visibleRoot string, dir chainDir, name string) (instructionFile, bool, error) {
	rel := path.Join(dir.rel, name)
	visible := path.Join(dir.visible, name)
	resolved, info, _, err := resolvePath(root, rel, 0, false)
	if errors.Is(err, ErrNotFound) {
		return instructionFile{}, false, nil
	}
	if err != nil {
		return instructionFile{}, false, fmt.Errorf("project: instruction file %s: %w", visible, err)
	}
	if !info.Mode().IsRegular() {
		return instructionFile{}, false, fmt.Errorf("%w: instruction file %s is a non-regular file", ErrUnsafePath, visible)
	}
	return instructionFile{path: visible, resolved: path.Join(visibleRoot, resolved), rel: resolved, size: info.Size()}, true, nil
}

func instructionBoundError(files []instructionFile, total int64) error {
	var b strings.Builder
	for i, file := range files {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s (%d bytes)", file.path, file.size)
	}
	return fmt.Errorf("project: instruction files total %d bytes, over the %d-byte limit: %s", total, MaxInstructionBytes, b.String())
}

type skillScan struct {
	skills  []Skill
	skipped []Skip
	byName  map[string]Skill
}

func skillsIn(root *os.Root, workspace, visibleRoot string, chain []chainDir) ([]Skill, []Skip) {
	s := &skillScan{byName: map[string]Skill{}}
	for i := len(chain) - 1; i >= 0; i-- {
		rel := path.Join(chain[i].rel, ".agents", "skills")
		visible := path.Join(chain[i].visible, ".agents", "skills")
		resolved, info, _, err := resolvePath(root, rel, 0, false)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			s.skipped = append(s.skipped, Skip{Path: visible, Reason: err.Error()})
			continue
		}
		if !info.IsDir() {
			s.skipped = append(s.skipped, Skip{Path: visible, Reason: "skill path is not a directory"})
			continue
		}
		entries, err := readDir(root, workspace, resolved)
		if err != nil {
			s.skipped = append(s.skipped, Skip{Path: visible, Reason: "cannot read skill directory: " + err.Error()})
			continue
		}
		for _, entry := range entries {
			name := entry.Name()
			if name == "" || strings.HasPrefix(name, ".") || name == "node_modules" {
				continue
			}
			if !utf8.ValidString(name) {
				s.skipped = append(s.skipped, Skip{Path: visible, Reason: "skill directory name is not UTF-8"})
				continue
			}
			scanSkill(root, workspace, visibleRoot, visible, resolved, name, s)
		}
	}
	sort.Slice(s.skills, func(i, j int) bool { return s.skills[i].Name < s.skills[j].Name })
	sort.Slice(s.skipped, func(i, j int) bool {
		if s.skipped[i].Path != s.skipped[j].Path {
			return s.skipped[i].Path < s.skipped[j].Path
		}
		return s.skipped[i].Reason < s.skipped[j].Reason
	})
	return s.skills, s.skipped
}

func scanSkill(root *os.Root, workspace, visibleRoot, skillsVisible, skillsRel, name string, s *skillScan) {
	dirRel := path.Join(skillsRel, name)
	dirVisible := path.Join(skillsVisible, name)
	dirResolved, dirInfo, _, err := resolvePath(root, dirRel, 0, false)
	if err != nil {
		s.skipped = append(s.skipped, Skip{Path: dirVisible, Reason: err.Error()})
		return
	}
	if !dirInfo.IsDir() {
		return
	}
	rel := path.Join(dirResolved, "SKILL.md")
	visible := path.Join(dirVisible, "SKILL.md")
	resolved, info, _, err := resolvePath(root, rel, 0, false)
	if errors.Is(err, ErrNotFound) {
		return
	}
	if err != nil {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: err.Error()})
		return
	}
	if !info.Mode().IsRegular() {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: "skill file is not a regular file"})
		return
	}
	data, real, truncated, err := readPrefix(root, workspace, resolved, MaxSkillFileBytes)
	if err != nil {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: "cannot read skill file: " + err.Error()})
		return
	}
	meta, err := parseFrontmatter(data, truncated)
	if err != nil {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: err.Error()})
		return
	}
	derived := meta.Name == ""
	skillName := meta.Name
	if derived {
		skillName = name
	}
	if err := validateSkillName(skillName); err != nil {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: err.Error()})
		return
	}
	if err := validateSkillDescription(meta.Description); err != nil {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: err.Error()})
		return
	}
	if meta.DisableModelInvocation {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: "disable-model-invocation is true"})
		return
	}
	if derived {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: "name derived from the skill directory"})
	}
	resolvedVisible, err := visibleHostPath(workspace, visibleRoot, real)
	if err != nil {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: err.Error()})
		return
	}
	if !skillPathsSafe(visible, resolvedVisible) {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: "skill path contains a control character"})
		return
	}
	if previous, ok := s.byName[skillName]; ok {
		s.skipped = append(s.skipped, Skip{Path: visible, Reason: fmt.Sprintf("duplicate name %q; kept %s", skillName, previous.Path)})
		return
	}
	skill := Skill{Name: skillName, Description: strings.TrimSpace(meta.Description), Path: visible, ResolvedPath: differ(visible, resolvedVisible)}
	s.byName[skillName] = skill
	s.skills = append(s.skills, skill)
}

func missingPath(candidate string, err error) (string, os.FileInfo, bool, error) {
	if !errors.Is(err, os.ErrNotExist) {
		return "", nil, false, err
	}
	return "", nil, false, fmt.Errorf("%w: %s", ErrNotFound, candidate)
}

// resolvePath follows relative symbolic links inside root component by
// component. A link's immediate target must exist: a missing child below an
// existing directory (including through a directory link) is simply absent.
func resolvePath(root *os.Root, rel string, depth int, linked bool) (string, os.FileInfo, bool, error) {
	if depth > 40 {
		return "", nil, linked, fmt.Errorf("%w: too many symbolic links", ErrUnsafePath)
	}
	parts := make([]pathPart, 0, strings.Count(rel, "/")+1)
	for _, name := range strings.Split(rel, "/") {
		if name != "" {
			parts = append(parts, pathPart{name: name})
		}
	}
	return resolveParts(root, parts, "", depth, linked)
}

type pathPart struct {
	name     string
	fromLink bool
}

func resolveParts(root *os.Root, parts []pathPart, resolved string, depth int, linked bool) (string, os.FileInfo, bool, error) {
	if depth > 40 {
		return "", nil, linked, fmt.Errorf("%w: too many symbolic links", ErrUnsafePath)
	}
	for len(parts) > 0 {
		part := parts[0]
		parts = parts[1:]
		if part.name == "" || part.name == "." {
			continue
		}
		if part.name == ".." {
			var err error
			if resolved, err = parentPath(resolved); err != nil {
				return "", nil, linked, err
			}
			continue
		}
		candidate := path.Join(resolved, part.name)
		info, err := root.Lstat(candidate)
		if err != nil {
			return "", nil, linked, resolveMissing(part, candidate, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return resolveLink(root, parts, resolved, depth, candidate)
		}
		if len(parts) > 0 && !info.IsDir() {
			return "", nil, linked, fmt.Errorf("%w: %s is not a directory", ErrUnsafePath, candidate)
		}
		resolved = candidate
	}
	if resolved == "" {
		info, err := root.Lstat(".")
		return ".", info, linked, err
	}
	info, err := root.Stat(resolved)
	return resolved, info, linked, err
}

func resolveMissing(part pathPart, candidate string, err error) error {
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if part.fromLink {
		return fmt.Errorf("%w: %s", ErrDangling, candidate)
	}
	return fmt.Errorf("%w: %s", ErrNotFound, candidate)
}

func resolveLink(root *os.Root, parts []pathPart, resolved string, depth int, candidate string) (string, os.FileInfo, bool, error) {
	target, trailing, err := linkParts(root, candidate)
	if err != nil {
		return "", nil, true, err
	}
	if !trailing {
		return resolveParts(root, append(target, parts...), resolved, depth+1, true)
	}
	targetPath, targetInfo, _, err := resolveParts(root, target, resolved, depth+1, true)
	if err != nil {
		return "", nil, true, err
	}
	if !targetInfo.IsDir() {
		return "", nil, true, fmt.Errorf("%w: link %s target is not a directory", ErrUnsafePath, candidate)
	}
	return resolveParts(root, parts, targetPath, depth+1, true)
}

func parentPath(resolved string) (string, error) {
	if resolved == "" {
		return "", fmt.Errorf("%w: path leaves its workspace", ErrUnsafePath)
	}
	resolved = path.Dir(resolved)
	if resolved == "." {
		return "", nil
	}
	return resolved, nil
}

func linkParts(root *os.Root, candidate string) ([]pathPart, bool, error) {
	target, err := root.Readlink(candidate)
	if err != nil {
		return nil, false, err
	}
	if path.IsAbs(target) {
		return nil, false, fmt.Errorf("%w: absolute link %s", ErrUnsafePath, candidate)
	}
	parts := splitParts(strings.TrimSuffix(target, "/"))
	return parts, strings.HasSuffix(target, "/"), nil
}

func splitParts(rel string) []pathPart {
	parts := make([]pathPart, 0, strings.Count(rel, "/")+1)
	for _, name := range strings.Split(rel, "/") {
		if name != "" {
			parts = append(parts, pathPart{name: name, fromLink: true})
		}
	}
	return parts
}

func checkDir(root *os.Root, workspace, rel string) error {
	f, real, err := openDir(root, workspace, rel)
	if err != nil {
		return fmt.Errorf("%w: directory %s: %v", ErrUnsafePath, rel, err)
	}
	_ = real
	return f.Close()
}

func openDir(root *os.Root, workspace, rel string) (*os.File, string, error) {
	f, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, "", err
	}
	real, err := descriptorPath(f)
	if err == nil && !withinHost(workspace, real) {
		err = fmt.Errorf("%w: directory %s resolves to %s", ErrUnsafePath, rel, real)
	}
	if err != nil {
		f.Close()
		return nil, "", err
	}
	return f, real, nil
}

func readDir(root *os.Root, workspace, rel string) ([]os.DirEntry, error) {
	f, _, err := openDir(root, workspace, rel)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	entries, err := f.ReadDir(-1)
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

func readPrefix(root *os.Root, workspace, rel string, limit int64) ([]byte, string, bool, error) {
	f, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, "", false, err
	}
	defer f.Close()
	real, err := descriptorPath(f)
	if err != nil {
		return nil, "", false, err
	}
	if !withinHost(workspace, real) {
		return nil, "", false, fmt.Errorf("%w: %s resolves to %s", ErrUnsafePath, rel, real)
	}
	if forbiddenTarget(real) {
		return nil, "", false, fmt.Errorf("%w: %s resolves to %s", ErrForbidden, rel, real)
	}
	info, err := f.Stat()
	if err != nil {
		return nil, "", false, err
	}
	if !info.Mode().IsRegular() {
		return nil, "", false, errors.New("not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, "", false, err
	}
	if int64(len(data)) > limit {
		return data[:limit], real, true, nil
	}
	return data, real, false, nil
}

func readExact(root *os.Root, workspace, rel string, want int64) ([]byte, string, error) {
	data, real, truncated, err := readPrefix(root, workspace, rel, want)
	if err != nil {
		return nil, "", err
	}
	if truncated || int64(len(data)) != want {
		return nil, "", errors.New("file changed while it was read")
	}
	return data, real, nil
}

func firstSkills(skills []Skill) []Skill {
	if len(skills) > MaxSnapshotSkills {
		skills = skills[:MaxSnapshotSkills]
	}
	return skills
}

func firstSkips(skipped []Skip) []Skip {
	if len(skipped) > MaxSnapshotSkipped {
		skipped = skipped[:MaxSnapshotSkipped]
	}
	out := make([]Skip, len(skipped))
	for i, skip := range skipped {
		out[i] = Skip{Path: strings.ToValidUTF8(skip.Path, "\uFFFD"), Reason: truncate(strings.ToValidUTF8(skip.Reason, "\uFFFD"), MaxSkipReasonBytes)}
	}
	return out
}

// FitEncoded returns s within limit encoded JSON bytes. Instruction content
// and the rendered index are required; optional skill and diagnostic pages are
// dropped, with their totals retained, if escaping would otherwise exceed the
// transport budget.
func (s Snapshot) FitEncoded(limit int) (Snapshot, error) {
	data, err := json.Marshal(s)
	if err != nil {
		return Snapshot{}, err
	}
	if len(data)+1 <= limit {
		return s, nil
	}
	s.Skipped = nil
	if data, err = json.Marshal(s); err != nil {
		return Snapshot{}, err
	}
	if len(data)+1 <= limit {
		return s, nil
	}
	s.Skills = nil
	if data, err = json.Marshal(s); err != nil {
		return Snapshot{}, err
	}
	if len(data)+1 <= limit {
		return s, nil
	}
	return Snapshot{}, fmt.Errorf("project: encoded snapshot exceeds %d bytes", limit)
}

// Meta returns the content-free projection of s.
func (s Snapshot) Meta() Meta {
	m := Meta{Root: s.Root, Skills: s.Skills, SkillsTotal: s.SkillsTotal, Skipped: s.Skipped, SkippedTotal: s.SkippedTotal,
		SkillsIndexBytes: len(s.SkillsIndexText()), SkillsIndexSHA256: s.SkillsIndexSHA256, SkillsOmitted: s.SkillsOmitted}
	for _, in := range s.Instructions {
		m.InstructionBytes += in.Size
		m.Instructions = append(m.Instructions, MetaFile{Path: in.Path, ResolvedPath: in.ResolvedPath, Size: in.Size, SHA256: in.SHA256})
	}
	return m
}

// Validate checks a metadata projection before it is shown.
func (m Meta) Validate() error {
	if err := m.validateShape(); err != nil {
		return err
	}
	if err := m.validateInstructions(); err != nil {
		return err
	}
	return m.validateSkills()
}

func (m Meta) validateShape() error {
	if m.Root != "" && (!path.IsAbs(m.Root) || path.Clean(m.Root) != m.Root) {
		return errors.New("project: metadata root must be an absolute clean path")
	}
	if len(m.Instructions) > 0 && m.Root == "" {
		return errors.New("project: metadata instructions have no root")
	}
	if len(m.Skills) > 0 && m.Root == "" {
		return errors.New("project: metadata skills have no root")
	}
	if len(m.Skills) > MaxSnapshotSkills || len(m.Skipped) > MaxSnapshotSkipped || m.SkillsTotal < len(m.Skills) || m.SkippedTotal < len(m.Skipped) {
		return errors.New("project: metadata exceeds its page or contradicts its totals")
	}
	if m.SkillsIndexBytes < 0 || m.SkillsIndexBytes > MaxSkillsIndexBytes || m.SkillsOmitted < 0 || m.SkillsOmitted > m.SkillsTotal {
		return errors.New("project: invalid metadata index counts")
	}
	if (m.SkillsTotal == 0) != (m.SkillsIndexBytes == 0) {
		return errors.New("project: metadata skills and index disagree")
	}
	if m.SkillsIndexBytes > 0 && !validDigest(m.SkillsIndexSHA256) || m.SkillsIndexBytes == 0 && m.SkillsIndexSHA256 != "" {
		return errors.New("project: metadata index hash disagrees with its bytes")
	}
	return nil
}

func (m Meta) validateInstructions() error {
	sum := 0
	paths := map[string]bool{}
	for _, in := range m.Instructions {
		if !visibleWithin(m.Root, in.Path) || (in.ResolvedPath != "" && !visibleWithin(m.Root, in.ResolvedPath)) {
			return fmt.Errorf("project: metadata instruction path %q is outside %q", in.Path, m.Root)
		}
		if paths[in.Path] {
			return fmt.Errorf("project: duplicate metadata instruction %q", in.Path)
		}
		paths[in.Path] = true
		if in.Size < 0 || !validDigest(in.SHA256) {
			return fmt.Errorf("project: metadata instruction %q is incomplete", in.Path)
		}
		sum += in.Size
	}
	if m.InstructionBytes != sum {
		return errors.New("project: metadata instruction bytes disagree with its files")
	}
	return nil
}

func (m Meta) validateSkills() error {
	names := map[string]bool{}
	for _, skill := range m.Skills {
		if err := validateSkillName(skill.Name); err != nil {
			return err
		}
		if err := validateSkillDescription(skill.Description); err != nil {
			return err
		}
		if names[skill.Name] {
			return fmt.Errorf("project: duplicate metadata skill %q", skill.Name)
		}
		names[skill.Name] = true
		if !visibleWithin(m.Root, skill.Path) || (skill.ResolvedPath != "" && !visibleWithin(m.Root, skill.ResolvedPath)) {
			return fmt.Errorf("project: metadata skill path %q is outside %q", skill.Path, m.Root)
		}
	}
	for _, skip := range m.Skipped {
		if skip.Reason == "" || len(skip.Reason) > MaxSkipReasonBytes || !utf8.ValidString(skip.Reason) {
			return errors.New("project: metadata skip has no bounded reason")
		}
	}
	return nil
}

func validDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}

func forbiddenTarget(rel string) bool {
	for _, component := range strings.Split(rel, "/") {
		if component == ".env" || strings.HasPrefix(component, ".env.") {
			return true
		}
	}
	return false
}

func withinHost(root, p string) bool {
	rel, err := filepath.Rel(root, p)
	return err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func visibleHostPath(workspace, visibleRoot, real string) (string, error) {
	rel, err := filepath.Rel(workspace, real)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %s is outside the workspace", ErrUnsafePath, real)
	}
	return path.Join(visibleRoot, filepath.ToSlash(rel)), nil
}

func visibleWithin(root, p string) bool {
	return p == root || root == "/" || strings.HasPrefix(p, root+"/")
}

// BoundDiagnostic clips a diagnostic to the projection bound.
func BoundDiagnostic(s string) string { return truncate(s, MaxDiagnosticBytes) }

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func differ(a, b string) string {
	if a == b {
		return ""
	}
	return b
}

// IndexEntry is one skill in the structured helper-host index. The index is
// newline-delimited JSON: one entry per line, then one trailer carrying the
// counts. The host renders model-facing text only after validating entries.
type IndexEntry struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	Path         string `json:"path"`
	ResolvedPath string `json:"resolved_path,omitempty"`
}

type indexTrailer struct {
	Entries int `json:"entries"`
	Omitted int `json:"omitted"`
	Skipped int `json:"skipped"`
}

// Index is the structured skills index persisted beside a run and passed from
// the helper to the host.
type Index struct {
	Entries []IndexEntry
	Omitted int
	Skipped int
}

// Encode returns the newline-delimited structured encoding of i.
func (i Index) Encode() string {
	var b strings.Builder
	for _, e := range i.Entries {
		data, err := json.Marshal(e)
		if err != nil {
			return ""
		}
		b.Write(data)
		b.WriteByte('\n')
	}
	data, err := json.Marshal(indexTrailer{Entries: len(i.Entries), Omitted: i.Omitted, Skipped: i.Skipped})
	if err != nil {
		return ""
	}
	b.Write(data)
	return b.String()
}

// DecodeIndex parses a structured index. Distinct entry and trailer schemas,
// strict required fields, unique keys, and the trailer-last rule make every
// line unambiguous.
func DecodeIndex(encoded string) (Index, error) {
	if encoded == "" {
		return Index{}, errors.New("project: empty skills index")
	}
	lines := strings.Split(encoded, "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) == 0 {
		return Index{}, errors.New("project: empty skills index")
	}
	trailer, err := decodeIndexTrailer(lines[len(lines)-1])
	if err != nil {
		return Index{}, fmt.Errorf("project: malformed skills index trailer: %w", err)
	}
	if len(lines)-1 != trailer.Entries {
		return Index{}, fmt.Errorf("project: skills index has %d entries, trailer says %d", len(lines)-1, trailer.Entries)
	}
	entries := make([]IndexEntry, 0, trailer.Entries)
	for _, line := range lines[:len(lines)-1] {
		if line == "" {
			return Index{}, errors.New("project: empty skills index line")
		}
		entry, err := decodeIndexEntry(line)
		if err != nil {
			return Index{}, fmt.Errorf("project: malformed skills index entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return Index{Entries: entries, Omitted: trailer.Omitted, Skipped: trailer.Skipped}, nil
}

func decodeIndexTrailer(line string) (indexTrailer, error) {
	fields, err := decodeIndexObject(line, []string{"entries", "omitted", "skipped"}, nil)
	if err != nil {
		return indexTrailer{}, err
	}
	entries, err := indexInt(fields, "entries")
	if err != nil {
		return indexTrailer{}, err
	}
	omitted, err := indexInt(fields, "omitted")
	if err != nil {
		return indexTrailer{}, err
	}
	skipped, err := indexInt(fields, "skipped")
	if err != nil {
		return indexTrailer{}, err
	}
	return indexTrailer{Entries: entries, Omitted: omitted, Skipped: skipped}, nil
}

func decodeIndexEntry(line string) (IndexEntry, error) {
	fields, err := decodeIndexObject(line, []string{"name", "description", "path"}, []string{"resolved_path"})
	if err != nil {
		return IndexEntry{}, err
	}
	name, err := indexString(fields, "name")
	if err != nil {
		return IndexEntry{}, err
	}
	description, err := indexString(fields, "description")
	if err != nil {
		return IndexEntry{}, err
	}
	path, err := indexString(fields, "path")
	if err != nil {
		return IndexEntry{}, err
	}
	resolved, err := indexString(fields, "resolved_path")
	if err != nil {
		return IndexEntry{}, err
	}
	return IndexEntry{Name: name, Description: description, Path: path, ResolvedPath: resolved}, nil
}

func decodeIndexObject(line string, required, optional []string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(strings.NewReader(line))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return nil, errors.New("not a JSON object")
	}
	allowed := map[string]bool{}
	for _, key := range append(append([]string{}, required...), optional...) {
		allowed[key] = true
	}
	fields := map[string]json.RawMessage{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("invalid object key")
		}
		if fields[key] != nil {
			return nil, fmt.Errorf("duplicate field %q", key)
		}
		if !allowed[key] {
			return nil, fmt.Errorf("unknown field %q", key)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, err
		}
		fields[key] = raw
	}
	token, err = decoder.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return nil, errors.New("unterminated object")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("trailing data")
		}
		return nil, err
	}
	for _, key := range required {
		if fields[key] == nil {
			return nil, fmt.Errorf("missing field %q", key)
		}
	}
	return fields, nil
}

func indexString(fields map[string]json.RawMessage, key string) (string, error) {
	raw, ok := fields[key]
	if !ok {
		return "", nil
	}
	if string(raw) == "null" {
		return "", fmt.Errorf("field %q is null", key)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("field %q is not a string", key)
	}
	return value, nil
}

func indexInt(fields map[string]json.RawMessage, key string) (int, error) {
	raw := fields[key]
	if string(raw) == "null" {
		return 0, fmt.Errorf("field %q is null", key)
	}
	var value int
	if err := json.Unmarshal(raw, &value); err != nil || value < 0 {
		return 0, fmt.Errorf("field %q is not a non-negative integer", key)
	}
	return value, nil
}

// Text renders the model-facing index from structured entries.
func (i Index) Text() string {
	var b strings.Builder
	lines := make([]string, len(i.Entries))
	for n, entry := range i.Entries {
		lines[n] = indexEntryLine(entry)
	}
	for _, line := range lines {
		if b.Len()+len(line) > MaxSkillsIndexBytes {
			break
		}
		b.WriteString(line)
	}
	if i.Omitted > 0 {
		available := MaxSkillsIndexBytes - b.Len()
		if available > 0 {
			b.WriteString(truncate(omittedSuffix(i.Omitted), available))
		}
	}
	return b.String()
}

func indexEntryLine(entry IndexEntry) string {
	path := entry.Path
	if entry.ResolvedPath != "" {
		path += " (resolves to " + entry.ResolvedPath + ")"
	}
	return fmt.Sprintf("- %s: %s\n  Path: %s\n", entry.Name, oneLine(entry.Description), path)
}

// buildIndex selects the entries that fit the bounded model-facing page and
// records the count it omitted. The structured encoding is built from those
// entries, never from the rendered text.
func buildIndex(skills []Skill, skipped int) Index {
	entries := make([]IndexEntry, 0, len(skills))
	lines := make([]string, 0, len(skills))
	for _, skill := range skills {
		entry := IndexEntry{Name: skill.Name, Description: oneLine(skill.Description), Path: skill.Path, ResolvedPath: skill.ResolvedPath}
		entries = append(entries, entry)
		lines = append(lines, indexEntryLine(entry))
	}
	footerBudget := MaxIndexFooterBytes
	if sumLen(lines) <= MaxSkillsIndexBytes {
		footerBudget = 0
	}
	skillBudget := MaxSkillsIndexBytes - footerBudget
	if skillBudget < 0 {
		skillBudget = MaxSkillsIndexBytes
	}
	kept, used := 0, 0
	for _, line := range lines {
		if used+len(line) > skillBudget {
			break
		}
		used += len(line)
		kept++
	}
	if kept == 0 && len(lines) > 0 && len(lines[0]) <= MaxSkillsIndexBytes {
		kept = 1
	}
	return Index{Entries: entries[:kept], Omitted: len(entries) - kept, Skipped: skipped}
}

func omittedSuffix(n int) string {
	if n <= 0 {
		return ""
	}
	suffix := fmt.Sprintf("%d more skills are available in the workspace.\n", n)
	return truncate(suffix, MaxIndexFooterBytes)
}

func truncate(s string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(s) <= limit {
		return s
	}
	const ellipsis = "…"
	if limit <= len(ellipsis) {
		return ""
	}
	end := limit - len(ellipsis)
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	if end == 0 {
		return ""
	}
	return s[:end] + ellipsis
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

func indexSafePath(p string) bool {
	for _, r := range p {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

func skillPathsSafe(visible, resolved string) bool {
	return indexSafePath(visible) && indexSafePath(resolved)
}

func sumLen(lines []string) int {
	total := 0
	for _, line := range lines {
		total += len(line)
	}
	return total
}

// Validate checks a snapshot read from the journal before it is rendered.
func (s Snapshot) Validate() error {
	if err := s.validateShape(); err != nil {
		return err
	}
	if err := s.validateInstructions(); err != nil {
		return err
	}
	return s.validateSkills()
}

func (s Snapshot) validateShape() error {
	if s.Root != "" && (!path.IsAbs(s.Root) || path.Clean(s.Root) != s.Root) {
		return errors.New("project: snapshot root must be an absolute clean path")
	}
	if len(s.Instructions) > 0 && s.Root == "" {
		return errors.New("project: snapshot instructions have no root")
	}
	if len(s.Instructions) > MaxInstructionFiles {
		return errors.New("project: snapshot holds too many instruction files")
	}
	if len(s.Skills) > 0 && s.Root == "" {
		return errors.New("project: snapshot skills have no root")
	}
	if (s.SkillsTotal == 0) != (s.SkillsIndex == "") {
		return errors.New("project: skills and their index disagree")
	}
	if s.SkillsIndex != "" {
		if _, err := DecodeIndex(s.SkillsIndex); err != nil {
			return err
		}
	}
	if s.SkillsOmitted < 0 || s.SkillsOmitted > s.SkillsTotal || s.SkillsTotal < len(s.Skills) || s.SkippedTotal < len(s.Skipped) {
		return errors.New("project: invalid skill counts")
	}
	if len(s.Skills) > MaxSnapshotSkills || len(s.Skipped) > MaxSnapshotSkipped {
		return errors.New("project: snapshot page exceeds its bound")
	}
	return nil
}

func (s Snapshot) validateInstructions() error {
	total := 0
	paths := map[string]bool{}
	for _, in := range s.Instructions {
		if !visibleWithin(s.Root, in.Path) || (in.ResolvedPath != "" && !visibleWithin(s.Root, in.ResolvedPath)) {
			return fmt.Errorf("project: instruction path %q is outside %q", in.Path, s.Root)
		}
		if paths[in.Path] {
			return fmt.Errorf("project: duplicate instruction path %q", in.Path)
		}
		paths[in.Path] = true
		if !utf8.ValidString(in.Content) || in.Size != len(in.Content) {
			return fmt.Errorf("project: instruction %q has an inconsistent size or encoding", in.Path)
		}
		if in.SHA256 != digest([]byte(in.Content)) {
			return fmt.Errorf("project: instruction %q has an inconsistent digest", in.Path)
		}
		total += in.Size
	}
	if total > MaxInstructionBytes {
		return fmt.Errorf("project: instructions total %d bytes, over %d", total, MaxInstructionBytes)
	}
	return nil
}

func (s Snapshot) validateSkills() error {
	if err := s.validateSkillPage(); err != nil {
		return err
	}
	if s.SkillsTotal == 0 {
		if s.SkillsIndex != "" {
			return errors.New("project: empty skills index expected")
		}
		return nil
	}
	index, err := DecodeIndex(s.SkillsIndex)
	if err != nil {
		return err
	}
	return s.validateSkillsIndex(index)
}

func (s Snapshot) validateSkillPage() error {
	names := map[string]bool{}
	for _, skill := range s.Skills {
		if err := validateSkillName(skill.Name); err != nil {
			return err
		}
		if err := validateSkillDescription(skill.Description); err != nil {
			return err
		}
		if names[skill.Name] {
			return fmt.Errorf("project: duplicate skill %q", skill.Name)
		}
		names[skill.Name] = true
		if !visibleWithin(s.Root, skill.Path) || (skill.ResolvedPath != "" && !visibleWithin(s.Root, skill.ResolvedPath)) {
			return fmt.Errorf("project: skill path %q is outside %q", skill.Path, s.Root)
		}
	}
	for _, skip := range s.Skipped {
		if skip.Reason == "" || len(skip.Reason) > MaxSkipReasonBytes || !utf8.ValidString(skip.Reason) {
			return errors.New("project: skipped skill has no bounded reason")
		}
	}
	return nil
}

func (s Snapshot) validateSkillsIndex(index Index) error {
	text := index.Text()
	if len(text) > MaxSkillsIndexBytes || !utf8.ValidString(text) {
		return errors.New("project: skills index exceeds its bound or is not UTF-8")
	}
	if s.SkillsIndexSHA256 != digest([]byte(text)) {
		return errors.New("project: skills index hash is inconsistent")
	}
	if index.Omitted != s.SkillsOmitted || len(index.Entries)+index.Omitted != s.SkillsTotal || index.Skipped != s.SkippedTotal {
		return errors.New("project: skills index counts are inconsistent")
	}
	names := map[string]bool{}
	for _, entry := range index.Entries {
		if err := validateSkillName(entry.Name); err != nil {
			return err
		}
		if err := validateSkillDescription(entry.Description); err != nil {
			return err
		}
		if names[entry.Name] {
			return fmt.Errorf("project: duplicate skill %q", entry.Name)
		}
		names[entry.Name] = true
		if !visibleWithin(s.Root, entry.Path) || (entry.ResolvedPath != "" && !visibleWithin(s.Root, entry.ResolvedPath)) {
			return fmt.Errorf("project: skill path %q is outside %q", entry.Path, s.Root)
		}
		if !skillPathsSafe(entry.Path, entry.ResolvedPath) {
			return fmt.Errorf("project: skill path %q contains a control character", entry.Path)
		}
	}
	return nil
}

// SkillsIndexText renders the persisted structured index for the model.
func (s Snapshot) SkillsIndexText() string {
	index, err := DecodeIndex(s.SkillsIndex)
	if err != nil {
		return ""
	}
	return index.Text()
}
