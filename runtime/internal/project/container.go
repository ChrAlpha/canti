package project

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"unicode/utf8"
)

// ContainerRequest names the workspace mount point and working directory as
// the job's container sees them. ReadContainer runs inside canti-job, so
// ordinary kernel resolution sees exactly the job's mounts.
type ContainerMount struct {
	Target   string `json:"target"`
	Readable bool   `json:"readable"`
}

type ContainerRequest struct {
	Root    string
	Workdir string
	Mounts  []ContainerMount
}

// ReadContainer binds the instruction files and skills of req using ordinary
// kernel resolution. Every opened descriptor is checked against Root through
// /proc/self/fd, so a mount or symlink cannot read outside the workspace.
func ReadContainer(ctx context.Context, req ContainerRequest) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := req.validate(); err != nil {
		return Snapshot{}, err
	}
	chain, err := containerChain(ctx, req.Root, req.Workdir, req.Mounts)
	if err != nil {
		return Snapshot{}, err
	}
	s := Snapshot{Root: req.Root}
	if s.Instructions, err = containerInstructions(ctx, req.Root, chain, req.Mounts); err != nil {
		return Snapshot{}, err
	}
	skills, skipped := containerSkills(ctx, req.Root, chain, req.Mounts)
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
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
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	return s, s.Validate()
}

func (r ContainerRequest) validate() error {
	if r.Root == "" || !path.IsAbs(r.Root) || path.Clean(r.Root) != r.Root {
		return errors.New("project: container root must be an absolute clean path")
	}
	if r.Workdir == "" || !path.IsAbs(r.Workdir) || path.Clean(r.Workdir) != r.Workdir || !visibleWithin(r.Root, r.Workdir) {
		return errors.New("project: container working directory must be inside the root")
	}
	return nil
}

type containerDir struct {
	visible string
	real    string
}

func containerChain(ctx context.Context, root, workdir string, mounts []ContainerMount) ([]containerDir, error) {
	rel := strings.TrimPrefix(workdir, root)
	rel = strings.TrimPrefix(rel, "/")
	out := make([]containerDir, 0, 1)
	seen := map[string]bool{}
	rootFile, rootReal, err := openContainerDir(root)
	if err != nil {
		return nil, err
	}
	rootFile.Close()
	if !withinContainer(root, rootReal) {
		return nil, fmt.Errorf("%w: workspace resolves to %s", ErrUnsafePath, rootReal)
	}
	if err := containerMount(root, mounts, rootReal); err != nil {
		return nil, err
	}
	seen[rootReal] = true
	out = append(out, containerDir{visible: root, real: rootReal})
	if rel == "" {
		return out, nil
	}
	prefix := ""
	for _, component := range strings.Split(rel, "/") {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if component == "" || component == "." {
			continue
		}
		prefix = path.Join(prefix, component)
		visible := path.Join(root, prefix)
		f, real, err := openContainerDir(visible)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("project: directory %s: %w", visible, err)
		}
		f.Close()
		if !withinContainer(root, real) {
			return nil, fmt.Errorf("%w: directory %s resolves to %s", ErrUnsafePath, visible, real)
		}
		if err := containerMount(root, mounts, real); err != nil {
			return nil, err
		}
		if seen[real] {
			continue
		}
		seen[real] = true
		out = append(out, containerDir{visible: visible, real: real})
	}
	return out, nil
}

func containerInstructions(ctx context.Context, root string, chain []containerDir, mounts []ContainerMount) ([]Instruction, error) {
	var found []containerFile
	for _, dir := range chain {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, name := range InstructionNames {
			file, ok, err := containerInstruction(ctx, root, dir, name, mounts)
			if err != nil {
				return nil, err
			}
			if ok {
				found = append(found, file)
				break
			}
		}
	}
	defer func() {
		for _, file := range found {
			file.file.Close()
		}
	}()
	if len(found) > MaxInstructionFiles {
		return nil, fmt.Errorf("project: %d instruction files, over the %d-file limit", len(found), MaxInstructionFiles)
	}
	var total int64
	for _, file := range found {
		total += file.size
	}
	if total > MaxInstructionBytes {
		var b strings.Builder
		for i, file := range found {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%s (%d bytes)", file.path, file.size)
		}
		return nil, fmt.Errorf("project: instruction files total %d bytes, over the %d-byte limit: %s", total, MaxInstructionBytes, b.String())
	}
	out := make([]Instruction, 0, len(found))
	for _, file := range found {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		content, err := readContainerExact(file)
		if err != nil {
			return nil, fmt.Errorf("project: read instruction file %s: %w", file.path, err)
		}
		if !utf8.Valid(content) {
			return nil, fmt.Errorf("project: instruction file %s is not valid UTF-8", file.path)
		}
		out = append(out, Instruction{Path: file.path, ResolvedPath: differ(file.path, file.resolved), Size: len(content), SHA256: digest(content), Content: string(content)})
	}
	return out, nil
}

type containerFile struct {
	path, resolved string
	file           *os.File
	size           int64
}

func containerInstruction(ctx context.Context, root string, dir containerDir, name string, mounts []ContainerMount) (containerFile, bool, error) {
	visible := path.Join(dir.visible, name)
	candidate := path.Join(dir.real, name)
	info, err := os.Lstat(candidate)
	if errors.Is(err, os.ErrNotExist) {
		return containerFile{}, false, nil
	}
	if err != nil {
		return containerFile{}, false, fmt.Errorf("project: inspect instruction file %s: %w", visible, err)
	}
	f, err := os.OpenFile(candidate, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		if info.Mode()&os.ModeSymlink != 0 {
			return containerFile{}, false, fmt.Errorf("%w: %s", ErrDangling, visible)
		}
		return containerFile{}, false, nil
	}
	if err != nil {
		return containerFile{}, false, fmt.Errorf("project: open instruction file %s: %w", visible, err)
	}
	real, err := descriptorPath(f)
	if err == nil && !withinContainer(root, real) {
		err = fmt.Errorf("%w: instruction file %s resolves to %s", ErrUnsafePath, visible, real)
	}
	if err == nil {
		err = containerMount(root, mounts, real)
	}
	if err == nil && forbiddenTarget(real) {
		err = fmt.Errorf("%w: instruction file %s resolves to %s", ErrForbidden, visible, real)
	}
	var target os.FileInfo
	if err == nil {
		target, err = f.Stat()
	}
	if err != nil {
		f.Close()
		return containerFile{}, false, err
	}
	if target.IsDir() {
		f.Close()
		return containerFile{}, false, fmt.Errorf("%w: instruction file %s is a non-regular file", ErrUnsafePath, visible)
	}
	if !target.Mode().IsRegular() {
		f.Close()
		return containerFile{}, false, fmt.Errorf("%w: instruction file %s is a non-regular file", ErrUnsafePath, visible)
	}
	return containerFile{path: visible, resolved: differ(visible, real), file: f, size: target.Size()}, true, nil
}

func readContainerExact(file containerFile) ([]byte, error) {
	defer file.file.Close()
	data, err := io.ReadAll(io.LimitReader(file.file, file.size+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != file.size {
		return nil, errors.New("file changed while it was read")
	}
	return data, nil
}

type containerScan struct {
	skills  []Skill
	skipped []Skip
	byName  map[string]Skill
}

func (s *containerScan) skill(skill Skill, skip Skip) {
	if skip.Reason != "" {
		s.skipped = append(s.skipped, skip)
		return
	}
	if previous, ok := s.byName[skill.Name]; ok {
		s.skipped = append(s.skipped, Skip{Path: skill.Path, Reason: fmt.Sprintf("duplicate name %q; kept %s", skill.Name, previous.Path)})
		return
	}
	s.byName[skill.Name] = skill
	s.skills = append(s.skills, skill)
}

func containerSkills(ctx context.Context, root string, chain []containerDir, mounts []ContainerMount) ([]Skill, []Skip) {
	s := &containerScan{byName: map[string]Skill{}}
	for i := len(chain) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			s.skipped = append(s.skipped, Skip{Path: path.Join(chain[i].visible, ".agents", "skills"), Reason: err.Error()})
			break
		}
		visible := path.Join(chain[i].visible, ".agents", "skills")
		dir := path.Join(chain[i].real, ".agents", "skills")
		f, real, err := openContainerDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			s.skipped = append(s.skipped, Skip{Path: visible, Reason: err.Error()})
			continue
		}
		if !withinContainer(root, real) {
			f.Close()
			s.skipped = append(s.skipped, Skip{Path: visible, Reason: "skill directory resolves outside the workspace"})
			continue
		}
		if err := containerMount(root, mounts, real); err != nil {
			f.Close()
			s.skipped = append(s.skipped, Skip{Path: visible, Reason: err.Error()})
			continue
		}
		entries, err := f.ReadDir(-1)
		f.Close()
		if err != nil {
			s.skipped = append(s.skipped, Skip{Path: visible, Reason: "cannot read skill directory: " + err.Error()})
			continue
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

		for _, entry := range entries {
			name := entry.Name()
			if name == "" || strings.HasPrefix(name, ".") || name == "node_modules" {
				continue
			}
			if !utf8.ValidString(name) {
				s.skipped = append(s.skipped, Skip{Path: visible, Reason: "skill directory name is not UTF-8"})
				continue
			}
			containerSkill(ctx, root, visible, real, name, mounts, s)
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

func containerSkill(ctx context.Context, root, skillsVisible, skillsReal, name string, mounts []ContainerMount, s *containerScan) {
	if err := ctx.Err(); err != nil {
		s.skill(Skill{}, Skip{Path: path.Join(skillsVisible, name), Reason: err.Error()})
		return
	}
	dirVisible := path.Join(skillsVisible, name)
	dirReal := path.Join(skillsReal, name)
	f, real, err := openContainerDir(dirReal)
	if err != nil {
		s.skill(Skill{}, Skip{Path: dirVisible, Reason: err.Error()})
		return
	}
	f.Close()
	if !withinContainer(root, real) {
		s.skill(Skill{}, Skip{Path: dirVisible, Reason: "skill directory resolves outside the workspace"})
		return
	}
	if err := containerMount(root, mounts, real); err != nil {
		s.skill(Skill{}, Skip{Path: dirVisible, Reason: err.Error()})
		return
	}
	visible := path.Join(dirVisible, "SKILL.md")
	candidate := path.Join(real, "SKILL.md")
	info, err := os.Lstat(candidate)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		s.skill(Skill{}, Skip{Path: visible, Reason: err.Error()})
		return
	}
	file, err := os.OpenFile(candidate, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		if info.Mode()&os.ModeSymlink != 0 {
			s.skill(Skill{}, Skip{Path: visible, Reason: "dangling symbolic link"})
		}
		return
	}
	if err != nil {
		s.skill(Skill{}, Skip{Path: visible, Reason: err.Error()})
		return
	}
	defer file.Close()
	realFile, err := descriptorPath(file)
	if err == nil && !withinContainer(root, realFile) {
		err = fmt.Errorf("skill file resolves outside the workspace to %s", realFile)
	}
	if err == nil {
		err = containerMount(root, mounts, realFile)
	}
	if err == nil && forbiddenTarget(realFile) {
		err = errors.New("skill file resolves to an environment file")
	}
	var target os.FileInfo
	if err == nil {
		target, err = file.Stat()
	}
	if err != nil {
		s.skill(Skill{}, Skip{Path: visible, Reason: err.Error()})
		return
	}
	if !target.Mode().IsRegular() {
		s.skill(Skill{}, Skip{Path: visible, Reason: "skill file is not a regular file"})
		return
	}
	data, err := io.ReadAll(io.LimitReader(file, MaxSkillFileBytes+1))
	if err != nil {
		s.skill(Skill{}, Skip{Path: visible, Reason: "cannot read skill file: " + err.Error()})
		return
	}
	truncated := len(data) > MaxSkillFileBytes
	if truncated {
		data = data[:MaxSkillFileBytes]
	}
	meta, err := parseFrontmatter(data, truncated)
	if err != nil {
		s.skill(Skill{}, Skip{Path: visible, Reason: err.Error()})
		return
	}
	derived := meta.Name == ""
	skillName := meta.Name
	if derived {
		skillName = name
	}
	if err := validateSkillName(skillName); err != nil {
		s.skill(Skill{}, Skip{Path: visible, Reason: err.Error()})
		return
	}
	if err := validateSkillDescription(meta.Description); err != nil {
		s.skill(Skill{}, Skip{Path: visible, Reason: err.Error()})
		return
	}
	if meta.DisableModelInvocation {
		s.skill(Skill{}, Skip{Path: visible, Reason: "disable-model-invocation is true"})
		return
	}
	if derived {
		s.skill(Skill{}, Skip{Path: visible, Reason: "name derived from the skill directory"})
	}
	if !skillPathsSafe(visible, realFile) {
		s.skill(Skill{}, Skip{Path: visible, Reason: "skill path contains a control character"})
		return
	}
	skill := Skill{Name: skillName, Description: strings.TrimSpace(meta.Description), Path: visible, ResolvedPath: differ(visible, realFile)}
	s.skill(skill, Skip{})
}

func openContainerDir(name string) (*os.File, string, error) {
	f, err := os.OpenFile(name, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, "", err
	}
	real, err := descriptorPath(f)
	if err != nil {
		f.Close()
		return nil, "", err
	}
	return f, real, nil
}

func descriptorPath(f *os.File) (string, error) {
	real, err := os.Readlink("/proc/self/fd/" + strconv.FormatUint(uint64(f.Fd()), 10))
	if err != nil {
		return "", fmt.Errorf("project: resolve opened descriptor: %w", err)
	}
	if strings.HasSuffix(real, " (deleted)") {
		return "", errors.New("project: opened path was deleted")
	}
	return filepath.Clean(real), nil
}

func containerMount(root string, mounts []ContainerMount, real string) error {
	if len(mounts) == 0 {
		return nil
	}
	best, bestLen := -1, -1
	for i, m := range mounts {
		if real == m.Target && len(m.Target) > bestLen {
			best, bestLen = i, len(m.Target)
		} else if strings.HasPrefix(real, m.Target+"/") && len(m.Target) > bestLen {
			best, bestLen = i, len(m.Target)
		}
	}
	if best < 0 {
		return fmt.Errorf("%w: %s is under no mount", ErrUnreadableMount, real)
	}
	if !mounts[best].Readable {
		return fmt.Errorf("%w: %s is under mount %s", ErrUnreadableMount, real, mounts[best].Target)
	}
	return nil
}

func withinContainer(root, real string) bool {
	return real == root || root == "/" || strings.HasPrefix(real, root+"/")
}
