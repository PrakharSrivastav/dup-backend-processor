package errors

type FileError struct {
	Message string
	Code int
	Details string
}

func (f *FileError) Error() string  {
	return f.Message
}

func NewFileError(message string, code int, details string) *FileError  {
	return & FileError{
		Message: message,
		Code: code,
		Details:details,
	}
}