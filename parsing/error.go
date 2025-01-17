package parsing

import "fmt"

type InputStreamReacedEndError struct{}

func (e InputStreamReacedEndError) Error() string {
	return "input stream が終端に達しました."
}

type BadDelimiterError struct {
	value rune
}

func (e BadDelimiterError) Error() string {
	return fmt.Sprintf("不正な delimiter です: %d (%s)", e.value, string(e.value))
}

type BadIntConstantError struct {
	value string
}

func (e BadIntConstantError) Error() string {
	return fmt.Sprintf("不正な int constant です: %s", e.value)
}

type BadStrConstantError struct {
	value string
}

func (e BadStrConstantError) Error() string {
	return fmt.Sprintf("不正な str constant です: %s", e.value)
}

type BadKeywordError struct {
	value string
}

func (e BadKeywordError) Error() string {
	return fmt.Sprintf("不正な keyword です: %s", e.value)
}

type BadIdentifierError struct {
	value string
}

func (e BadIdentifierError) Error() string {
	return fmt.Sprintf("不正な identifier です: %s", e.value)
}
