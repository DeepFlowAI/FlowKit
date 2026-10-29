package textproc

import (
	"strconv"
	"strings"
)

// digitReading is how a digit is spoken when a number is read digit by digit.
// 1 is "幺", the telephony convention that keeps it apart from 7.
var digitReading = [10]string{"零", "幺", "二", "三", "四", "五", "六", "七", "八", "九"}

// idKeywords mark a nearby number as an identifier (phone, staff ID, account,
// code), which is read digit by digit rather than as a quantity.
var idKeywords = []string{
	"工号", "电话", "热线", "户号", "手机", "号码", "编号", "账号", "卡号",
	"验证码", "尾号", "单号", "客服", "拨打", "致电", "座机", "分机",
}

const (
	// keywordBefore / keywordAfter: how many runes around a digit run are
	// searched for an idKeyword ("工号1001", "96968热线").
	keywordBefore = 6
	keywordAfter  = 3
	// longDigits: runs at least this long are identifiers on their own
	// (phone numbers, order numbers); nobody says "一千三百八十万…".
	longDigits = 7
)

// ReadDigits spells out identifier-like numbers digit by digit in Chinese, so a
// model's normalizer does not read "工号10086" as "一万零八十六". A run of ASCII
// digits is spelled out when it is long (>= longDigits), starts with 0 (and has
// 3+ digits), or has 3+ digits with an idKeyword close by. Decimals and other
// numbers are left for the model to read as quantities. An identifier the LLM
// already wrote as a Chinese quantity ("工号八万六千零二十一") is respelled too.
func ReadDigits(text string) string {
	runes := []rune(respellNumeralIDs(text))
	var b strings.Builder
	b.Grow(len(text))
	for i := 0; i < len(runes); {
		if !isDigit(runes[i]) {
			b.WriteRune(runes[i])
			i++
			continue
		}
		// A run may contain single hyphens between digits ("027-12345678").
		j := i
		for j < len(runes) && (isDigit(runes[j]) || (runes[j] == '-' && j+1 < len(runes) && isDigit(runes[j+1]))) {
			j++
		}
		run := runes[i:j]
		if isIdentifier(runes, i, j) {
			for _, r := range run {
				if r != '-' {
					b.WriteString(digitReading[r-'0'])
				}
			}
		} else {
			b.WriteString(string(run))
		}
		i = j
	}
	return b.String()
}

func isDigit(r rune) bool { return r >= '0' && r <= '9' }

// numeralIDKeywords introduce a number that is always an identifier, so a
// Chinese quantity right after one ("工号八万六千零二十一") was meant digit by
// digit. Narrower than idKeywords: "客服一百人", "手机两千元" and "电话一百分钟"
// are quantities.
var numeralIDKeywords = []string{
	"工号", "户号", "号码", "编号", "账号", "卡号", "验证码", "尾号", "单号", "热线", "分机",
}

// quantityUnits right after a numeral mark it as an amount, not an identifier.
const quantityUnits = "元块角分秒钟个次人天月年吨度GgMm兆"

var cnDigit = map[rune]int{'零': 0, '〇': 0, '一': 1, '二': 2, '两': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
var cnUnit = map[rune]int{'十': 10, '百': 100, '千': 1000, '万': 10000}

// respellNumeralIDs rewrites a Chinese quantity numeral that directly follows a
// numeralIDKeyword (optionally after 是/为/a colon) into digit-by-digit reading.
// Numerals without a unit character are already digit by digit and are kept.
func respellNumeralIDs(text string) string {
	runes := []rune(text)
	var b strings.Builder
	b.Grow(len(text))
	for i := 0; i < len(runes); {
		k := keywordAt(runes, i)
		if k == 0 {
			b.WriteRune(runes[i])
			i++
			continue
		}
		j := i + k
		for j < len(runes) && strings.ContainsRune("是为：:", runes[j]) && j < i+k+2 {
			j++
		}
		e := j
		for e < len(runes) && (cnDigit[runes[e]] > 0 || runes[e] == '零' || runes[e] == '〇' || cnUnit[runes[e]] > 0) {
			e++
		}
		b.WriteString(string(runes[i:j]))
		unitAfter := e < len(runes) && strings.ContainsRune(quantityUnits, runes[e])
		if v, ok := parseCNQuantity(runes[j:e]); ok && v >= 100 && !unitAfter {
			for _, d := range strconv.Itoa(v) {
				b.WriteString(digitReading[d-'0'])
			}
		} else {
			b.WriteString(string(runes[j:e]))
		}
		i = e
	}
	return b.String()
}

// keywordAt returns the rune length of the numeralIDKeyword starting at i, or 0.
func keywordAt(runes []rune, i int) int {
	for _, k := range numeralIDKeywords {
		kr := []rune(k)
		if i+len(kr) <= len(runes) && string(runes[i:i+len(kr)]) == k {
			return len(kr)
		}
	}
	return 0
}

// parseCNQuantity parses a Chinese integer numeral that contains at least one
// unit character (十百千万), e.g. 八万六千零二十一 -> 86021.
func parseCNQuantity(rs []rune) (int, bool) {
	total, section, num := 0, 0, 0
	hasUnit := false
	for _, r := range rs {
		if u, ok := cnUnit[r]; ok {
			hasUnit = true
			if u == 10000 {
				total += (section + num) * u
				section, num = 0, 0
				continue
			}
			if num == 0 && u == 10 {
				num = 1 // 十五 = 15
			}
			section += num * u
			num = 0
			continue
		}
		num = cnDigit[r]
	}
	return total + section + num, hasUnit
}

// isIdentifier decides whether runes[i:j] (a digit run) is read digit by digit.
func isIdentifier(runes []rune, i, j int) bool {
	n := 0
	for _, r := range runes[i:j] {
		if isDigit(r) {
			n++
		}
	}
	if n < j-i {
		// Hyphenated digits are a phone or ID number, except a date (2024-10-27).
		p := strings.Split(string(runes[i:j]), "-")
		return !(len(p) == 3 && len(p[0]) == 4 && len(p[1]) <= 2 && len(p[2]) <= 2)
	}
	decimal := (i >= 2 && runes[i-1] == '.' && isDigit(runes[i-2])) ||
		(j+1 < len(runes) && runes[j] == '.' && isDigit(runes[j+1]))
	if decimal || n < 3 {
		return false
	}
	if n >= longDigits || runes[i] == '0' {
		return true
	}
	before := string(runes[max(0, i-keywordBefore):i])
	after := string(runes[j:min(len(runes), j+keywordAfter)])
	for _, k := range idKeywords {
		if strings.Contains(before, k) || strings.Contains(after, k) {
			return true
		}
	}
	return false
}
