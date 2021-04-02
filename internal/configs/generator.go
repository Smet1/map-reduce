package configs

type Generator struct {
	Letters    string  `yaml:"letters"`
	MinLentgh  int     `yaml:"min_lentgh"`
	MaxLentgh  int     `yaml:"max_lentgh"`
	FileSizeMB float64 `yaml:"file_size_mb"`
	Output     string  `yaml:"output"`
}
