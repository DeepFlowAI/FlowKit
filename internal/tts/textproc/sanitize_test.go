package textproc

import "testing"

func TestSanitize(t *testing.T) {
	cases := []struct{ in, want string }{
		{"建议 ≥ 100 元", "建议 大于等于 100 元"},  // comparison symbol → Chinese
		{"安装/启用水表", "安装或启用水表"},           // slash → 或
		{"原件 + 复印件", "原件 复印件"},           // plus dropped
		{"第一步\n第二步", "第一步 第二步"},          // newline folded
		{"## 标题 **粗体**", "标题 粗体"},        // markdown stripped
		{"填写《用水开户申请表》。", "填写用水开户申请表。"},   // brackets stripped, content kept
		{"购房合同（原件 + 复印件）", "购房合同原件 复印件"}, // parens + plus stripped
		{"1. 携带房产证；", "1. 携带房产证；"},       // list digit + CJK kept
		{"水价是3.14元。", "水价是3.14元。"},       // decimal kept
	}
	for _, c := range cases {
		if got := Sanitize(c.in); got != c.want {
			t.Errorf("Sanitize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
