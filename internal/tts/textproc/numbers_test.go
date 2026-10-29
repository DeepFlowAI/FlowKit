package textproc

import "testing"

func TestReadDigits(t *testing.T) {
	cases := []struct{ in, want string }{
		{"您的工号是1001。", "您的工号是幺零零幺。"},                         // keyword before
		{"请拨打96968热线咨询", "请拨打九六九六八热线咨询"},                     // keyword before and after
		{"可以拨打027-12345678咨询", "可以拨打零二七幺二三四五六七八咨询"},          // hyphenated phone number
		{"手机尾号8899的用户", "手机尾号八八九九的用户"},                       // 4 digits after 尾号
		{"联系13800138000", "联系幺三八零零幺三八零零零"},                   // 11 digits, no keyword
		{"套餐一百零九元，优惠30元", "套餐一百零九元，优惠30元"},                   // quantity untouched
		{"本月用水120吨", "本月用水120吨"},                             // 3 digits, no keyword
		{"水价是3.14元，电话里说的12345.6元", "水价是3.14元，电话里说的12345.6元"}, // decimals untouched
		{"共10人", "共10人"},                 // short run
		{"2024-10-27停水", "2024-10-27停水"}, // hyphenated date untouched
		// identifiers the LLM wrote as Chinese quantities
		{"我是移动客服，工号八万六千零二十一。", "我是移动客服，工号八六零二幺。"},
		{"工号是：八万六千零二十一", "工号是：八六零二幺"},
		{"热线九万六千九百六十八", "热线九六九六八"},
		{"尾号两千零一十", "尾号二零幺零"},
		{"工号是八六零二一，您放心。", "工号是八六零二一，您放心。"}, // already digit by digit
		{"客服一百人", "客服一百人"},                 // 客服 is not a numeral keyword
		{"号码十五", "号码十五"},                   // under 100: not an identifier
		{"电话费一百元", "电话费一百元"},               // numeral does not follow the keyword
		{"这款手机两千元", "这款手机两千元"},             // 手机 is not a numeral keyword
		{"号码一百个", "号码一百个"},                 // followed by a quantity unit
	}
	for _, c := range cases {
		if got := ReadDigits(c.in); got != c.want {
			t.Errorf("ReadDigits(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
