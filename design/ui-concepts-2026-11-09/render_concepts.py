from pathlib import Path
from PIL import Image, ImageDraw, ImageFont, ImageFilter
import math

OUT = Path(__file__).resolve().parent
CJK = '/System/Library/Fonts/Hiragino Sans GB.ttc'
MONO = '/System/Library/Fonts/Menlo.ttc'


class Canvas:
    def __init__(self, w, h, scale=2, bg='#FFFFFF'):
        self.w = w
        self.h = h
        self.s = scale
        self.image = Image.new('RGB', (round(w * scale), round(h * scale)), bg)
        self.draw = ImageDraw.Draw(self.image)
        self._fonts = {}

    def font(self, size, bold=False, mono=False):
        key = (size, bold, mono)
        if key not in self._fonts:
            path = MONO if mono else CJK
            index = 0 if mono else (2 if bold else 0)
            self._fonts[key] = ImageFont.truetype(path, max(1, round(size * self.s)), index=index)
        return self._fonts[key]

    def p(self, value):
        return round(value * self.s)

    def box(self, x, y, w, h):
        return (self.p(x), self.p(y), self.p(x + w), self.p(y + h))

    def rect(self, x, y, w, h, fill, outline=None, width=1):
        self.draw.rectangle(self.box(x, y, w, h), fill=fill, outline=outline, width=self.p(width))

    def rr(self, x, y, w, h, r, fill, outline=None, width=1):
        self.draw.rounded_rectangle(self.box(x, y, w, h), radius=self.p(r), fill=fill, outline=outline, width=self.p(width))

    def circle(self, x, y, d, fill, outline=None, width=1):
        self.draw.ellipse(self.box(x, y, d, d), fill=fill, outline=outline, width=self.p(width))

    def line(self, points, fill, width=1, joint='curve'):
        self.draw.line([(self.p(x), self.p(y)) for x, y in points], fill=fill, width=self.p(width), joint=joint)

    def polygon(self, points, fill):
        self.draw.polygon([(self.p(x), self.p(y)) for x, y in points], fill=fill)

    def text(self, x, y, text, size, fill, bold=False, mono=False, align='left'):
        anchor = {'left': 'lt', 'center': 'mt', 'right': 'rt'}[align]
        value = str(text)
        use_mono = mono and value.isascii()
        self.draw.text((self.p(x), self.p(y)), value, font=self.font(size, bold, use_mono), fill=fill, anchor=anchor)

    def width(self, text, size, bold=False, mono=False):
        value = str(text)
        use_mono = mono and value.isascii()
        return self.draw.textlength(value, font=self.font(size, bold, use_mono)) / self.s

    def wrap(self, text, width, size, bold=False, mono=False):
        lines = []
        for raw in str(text).split('\n'):
            line = ''
            for ch in raw:
                candidate = line + ch
                if line and self.width(candidate, size, bold, mono) > width:
                    lines.append(line)
                    line = ch
                else:
                    line = candidate
            lines.append(line)
        return lines

    def para(self, x, y, text, width, size, fill, line_height=None, bold=False, mono=False, align='left', max_lines=None):
        lh = line_height or size * 1.55
        lines = self.wrap(text, width, size, bold, mono)
        if max_lines:
            lines = lines[:max_lines]
        for i, line in enumerate(lines):
            tx = x if align == 'left' else (x + width / 2 if align == 'center' else x + width)
            self.text(tx, y + i * lh, line, size, fill, bold, mono, align)
        return y + len(lines) * lh

    def pill(self, x, y, text, size, fg, bg, pad_x=10, h=26, outline=None, bold=False, mono=False):
        w = self.width(text, size, bold, mono) + pad_x * 2
        self.rr(x, y, w, h, h / 2, bg, outline)
        self.text(x + w / 2, y + (h - size) / 2 - 1, text, size, fg, bold, mono, 'center')
        return w


def gradient(c, top, bottom):
    def rgb(h):
        h = h.lstrip('#')
        return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))
    a, b = rgb(top), rgb(bottom)
    for yy in range(c.image.height):
        t = yy / max(1, c.image.height - 1)
        color = tuple(round(a[i] * (1 - t) + b[i] * t) for i in range(3))
        c.draw.line([(0, yy), (c.image.width, yy)], fill=color)


def arrow(c, x, y, color, size=10, width=1.5):
    c.line([(x - size, y), (x + size, y)], color, width)
    c.line([(x + size - 5, y - 5), (x + size, y), (x + size - 5, y + 5)], color, width)


def back(c, x, y, color):
    c.line([(x + 9, y - 7), (x + 2, y), (x + 9, y + 7)], color, 1.7)
    c.line([(x + 2, y), (x + 17, y)], color, 1.7)


def copy_icon(c, x, y, color):
    c.rr(x + 4, y, 10, 12, 2, None if False else None, color, 1)
    # Paint over the helper's fallback with the two-sheet line icon.
    c.line([(x + 1, y + 3), (x + 1, y + 14), (x + 10, y + 14)], color, 1.2)


def nav_icon(c, kind, x, y, color):
    if kind == 0:
        for dx in (0, 8):
            for dy in (0, 8):
                c.rr(x + dx, y + dy, 5, 5, 1, color)
    elif kind == 1:
        c.line([(x, y + 3), (x + 7, y + 3), (x + 7, y + 12), (x + 15, y + 12)], color, 1.5)
        for px, py in ((x, y + 3), (x + 7, y + 3), (x + 7, y + 12), (x + 15, y + 12)):
            c.circle(px - 2, py - 2, 4, color)
    elif kind == 2:
        c.circle(x, y, 16, None if False else None, color, 1)
        c.text(x + 8, y + 1, '∑', 12, color, bold=True, align='center')
    elif kind == 3:
        c.rr(x, y, 16, 15, 3, None, color, 1)
        c.polygon([(x + 6, y + 4), (x + 12, y + 7.5), (x + 6, y + 11)], color)
    else:
        c.line([(x, y + 3), (x + 15, y + 3)], color, 1.5)
        c.line([(x, y + 8), (x + 15, y + 8)], color, 1.5)
        c.line([(x, y + 13), (x + 10, y + 13)], color, 1.5)


def bottom_nav(c, active, bg, fg, muted, accent, border, dark=True):
    c.rect(0, 776, 390, 68, bg)
    c.line([(0, 776), (390, 776)], border, 0.7)
    labels = ['概览', '流水线', '模型', '自动化', '结构']
    for i, label in enumerate(labels):
        cx = 39 + i * 78
        color = accent if i == active else muted
        if i == active:
            c.rr(cx - 24, 782, 48, 26, 13, fg)
        nav_icon(c, i, cx - 8, 787, color)
        c.text(cx, 815, label, 9, color, bold=(i == active), align='center')


def header_brand(c, theme, label='PROJECT HUB / 01'):
    c.rr(16, 20, 36, 36, 11, theme['accent'])
    c.text(34, 29, 'FM', 13, theme['accent_text'], bold=True, mono=True, align='center')
    c.text(62, 20, '足球经理的推断', 13, theme['text'], bold=True)
    c.text(62, 40, label, 7.5, theme['muted'], mono=True)
    c.circle(337, 27, 6, theme['accent'])
    c.text(348, 23, '快照', 9, theme['muted'])
    c.text(348, 39, '已打包', 9, theme['text'], bold=True)


def probability_bar(c, x, y, w, values, colors, h=7, gap=2, bg=None):
    total = sum(values)
    cursor = x
    for i, value in enumerate(values):
        seg = (w - gap * 2) * value / total
        c.rr(cursor, y, seg, h, h / 2, colors[i])
        cursor += seg + gap


def matrix(c, x, y, w, h, accent, base, hot=(3, 2), rounded=2):
    gap = 4
    cw = (w - gap * 6) / 7
    ch = (h - gap * 4) / 5
    values = [0.18, 0.25, 0.34, 0.48, 0.37, 0.22, 0.12]
    for row in range(5):
        for col in range(7):
            if (row, col) == hot:
                color = accent
            else:
                # Fixed stepped palette keeps both H5 and Canvas implementations simple.
                palette = [base, base, base, accent, base]
                color = palette[(row * 3 + col) % len(palette)] if values[col] > 0.3 else base
            c.rr(x + col * (cw + gap), y + row * (ch + gap), cw, ch, rounded, color)


def completeness(c, x, y, value, accent, track, text_color, label='数据完整度'):
    c.text(x, y, label, 9, text_color)
    c.text(x + 72, y - 1, f'{value}%', 10, text_color, bold=True, mono=True)
    c.rr(x, y + 18, 88, 5, 2.5, track)
    c.rr(x, y + 18, 88 * value / 100, 5, 2.5, accent)


def match_card_a(c, y=245, compact=False):
    x, w = 16, 358
    h = 302 if not compact else 168
    c.rr(x, y, w, h, 18, '#151C24', '#2B3742', 1)
    c.pill(x + 14, y + 14, '周日006', 9, '#C9F66F', '#243323', h=23, bold=True, mono=True)
    c.text(x + 100, y + 18, '意大利甲级联赛', 9, '#94A2AD')
    c.text(x + w - 14, y + 18, '19:30', 10, '#D8E0E5', mono=True, align='right')
    c.text(x + 14, y + 52, '那不勒斯', 20, '#F5F7F8', bold=True)
    c.text(x + 130, y + 57, 'VS', 9, '#657580', mono=True)
    c.text(x + 164, y + 52, '拉齐奥', 20, '#F5F7F8', bold=True)
    c.pill(x + w - 105, y + 49, '倾向 主胜', 10, '#14210F', '#C9F66F', h=28, bold=True)
    c.text(x + 14, y + 96, '三向概率', 9, '#82929E')
    c.text(x + w - 14, y + 91, '42.6%', 21, '#C9F66F', bold=True, mono=True, align='right')
    probability_bar(c, x + 14, y + 126, w - 28, [42.6, 28.0, 29.4], ['#C9F66F', '#53616D', '#8795A0'], h=8)
    labels = [('主胜', '42.6%', '#C9F66F'), ('平局', '28.0%', '#C2CBD1'), ('客胜', '29.4%', '#C2CBD1')]
    for i, (label, value, color) in enumerate(labels):
        xx = x + 14 + i * 111
        c.text(xx, y + 145, label, 9, '#82929E')
        c.text(xx + 34, y + 143, value, 11, color, bold=True, mono=True)
    c.line([(x + 14, y + 176), (x + w - 14, y + 176)], '#2A353F', 0.8)
    c.text(x + 14, y + 191, '比分参考', 9, '#82929E')
    c.pill(x + 79, y + 184, '1:1  13.3%', 9, '#E5EBEE', '#222D36', h=27, mono=True)
    c.pill(x + 177, y + 184, '1:0  10.8%', 9, '#E5EBEE', '#222D36', h=27, mono=True)
    completeness(c, x + 14, y + 229, 20, '#E8B85D', '#33404A', '#C8D1D6')
    c.pill(x + 123, y + 224, '赔率降级估计 · 弱参考', 9, '#E8C98B', '#332D22', h=25)
    c.circle(x + 15, y + 271, 12, '#4B3C24')
    c.text(x + 21, y + 271, '!', 9, '#F1C675', bold=True, align='center')
    c.text(x + 35, y + 269, '缺失数据：home_xg 等 7 项风险', 9, '#C8AB78')


def overview_a():
    t = {'accent': '#C9F66F', 'accent_text': '#15200F', 'text': '#F4F7F8', 'muted': '#82929E'}
    c = Canvas(390, 844, 2.4, '#0E141B')
    gradient(c, '#101922', '#0C1117')
    header_brand(c, t, 'TACTICAL DATA / 01')
    c.text(16, 84, 'DAILY PROBABILITY · 2026.11.08', 8, '#C9F66F', mono=True)
    c.text(16, 111, '让每一次推断，', 30, '#F4F7F8', bold=True)
    c.text(16, 151, '都有迹可循。', 30, '#C9F66F', bold=True)
    c.para(16, 199, '从官方对阵到赛后回测，保留概率、完整度与风险。', 350, 12, '#9BA9B3', 20)
    c.rr(16, 253, 358, 48, 13, '#C9F66F')
    c.text(32, 266, '查看每日推断', 13, '#15200F', bold=True)
    c.pill(202, 263, '25 场', 10, '#253518', '#DDFA9F', h=26, bold=True, mono=True)
    arrow(c, 344, 277, '#15200F', 9)
    c.rr(16, 311, 173, 42, 12, '#141D25', '#33414C')
    c.text(102, 323, '探索数据流水线', 11, '#D4DCE1', align='center')
    c.rr(201, 311, 173, 42, 12, '#141D25', '#33414C')
    c.text(287, 323, '复制每日命令', 11, '#D4DCE1', align='center')
    c.rr(16, 372, 358, 174, 18, '#131B23', '#2A3742')
    c.text(32, 388, 'MODEL BASELINE', 8, '#778995', mono=True)
    c.text(32, 408, 'poisson-dc-v1', 13, '#EDF2F4', bold=True, mono=True)
    c.pill(272, 386, '可复现基线', 9, '#C9F66F', '#203022', h=24)
    matrix(c, 32, 443, 198, 72, '#C9F66F', '#26362A')
    c.text(255, 448, '完整概率分布', 10, '#D5DEE2', bold=True)
    c.text(255, 470, '追加而非覆盖', 10, '#D5DEE2', bold=True)
    c.text(255, 497, '0—10 球网格 · 示意', 8, '#778995')
    stats = [('04', '报告格式'), ('08', '核心数据表'), ('05', '前列比分'), ('04', 'AI 策略')]
    for i, (value, label) in enumerate(stats):
        xx = 16 + i * 90
        c.text(xx, 568, value, 23, '#F2F6F7', bold=True, mono=True)
        c.text(xx, 600, label, 9, '#82929E')
    c.line([(16, 629), (374, 629)], '#293640', 0.8)
    c.text(16, 648, '四条核心设计原则', 15, '#F2F6F7', bold=True)
    principles = [('01', '官方对阵定义边界'), ('02', '推断版本只追加'), ('03', '时间边界防泄漏'), ('04', '异常显式中止')]
    for i, (num, label) in enumerate(principles):
        xx = 16 + (i % 2) * 182
        yy = 679 + (i // 2) * 40
        c.text(xx, yy, num, 9, '#C9F66F', mono=True)
        c.text(xx + 28, yy - 1, label, 10, '#B7C2C9', bold=True)
    bottom_nav(c, 0, '#10171E', '#243321', '#788995', '#C9F66F', '#27333D')
    return c


def daily_a():
    c = Canvas(390, 844, 2.4, '#0E141B')
    gradient(c, '#101922', '#0C1117')
    back(c, 16, 33, '#C9F66F')
    c.text(45, 21, '每日推断', 18, '#F4F7F8', bold=True)
    c.text(374, 26, '2026.11.08', 10, '#9BA9B3', mono=True, align='right')
    c.text(16, 65, '25 场比赛', 12, '#DCE4E8', bold=True)
    c.text(93, 65, '·', 12, '#657580')
    c.text(110, 65, '25 条策略', 12, '#DCE4E8', bold=True)
    c.pill(287, 59, 'Kimi / 稳健概率', 9, '#C9F66F', '#203022', h=26)
    c.rr(16, 101, 358, 66, 14, '#1A211B', '#3B4B2C')
    c.circle(30, 116, 20, '#C9F66F')
    c.text(40, 119, 'i', 11, '#15200F', bold=True, align='center')
    c.para(61, 113, '概率估计仅供分析，不构成收益保证。低完整度仅作弱参考。', 296, 10, '#B9C6AD', 17)
    c.text(16, 190, '今日比赛', 15, '#F4F7F8', bold=True)
    c.text(374, 194, '按开赛时间排序', 9, '#82929E', align='right')
    match_card_a(c, 224)
    # The next card enters the viewport to communicate vertical continuation.
    c.rr(16, 540, 358, 214, 18, '#151C24', '#2B3742')
    c.pill(30, 554, '周日007', 9, '#C9F66F', '#243323', h=23, bold=True, mono=True)
    c.text(116, 558, '西班牙甲级联赛', 9, '#94A2AD')
    c.text(360, 558, '21:00', 10, '#D8E0E5', mono=True, align='right')
    c.text(30, 592, '埃尔切', 19, '#F5F7F8', bold=True)
    c.text(123, 597, 'VS', 9, '#657580', mono=True)
    c.text(157, 592, '皇家贝蒂斯', 19, '#F5F7F8', bold=True)
    c.pill(283, 589, '倾向 客胜', 10, '#14210F', '#C9F66F', h=28, bold=True)
    c.text(30, 638, '最高概率', 9, '#82929E')
    c.text(360, 632, '55.7%', 21, '#C9F66F', bold=True, mono=True, align='right')
    probability_bar(c, 30, 671, 330, [18.7, 25.6, 55.7], ['#8795A0', '#53616D', '#C9F66F'], h=8)
    c.text(30, 694, '完整度 20%', 10, '#E8C98B', bold=True)
    c.text(360, 694, '赔率降级估计 · 弱参考', 9, '#E8C98B', align='right')
    return c


def light_header(c, title='足球经理的推断'):
    c.rr(16, 20, 36, 36, 8, '#163C34')
    c.text(34, 29, 'FM', 13, '#FFFFFF', bold=True, mono=True, align='center')
    c.text(62, 20, title, 13, '#152B27', bold=True)
    c.text(62, 40, 'PROBABILITY RESEARCH', 7.5, '#75827E', mono=True)
    c.pill(302, 24, '报告快照', 9, '#2D6555', '#E3EFEA', h=28)


def match_card_b(c, y=226):
    x, w = 16, 358
    c.rr(x, y, w, 316, 12, '#FFFFFF', '#D9DEDA')
    c.rect(x, y, 4, 316, '#2C765D')
    c.text(x + 16, y + 16, '周日006', 10, '#2C765D', bold=True, mono=True)
    c.text(x + 87, y + 16, '意大利甲级联赛', 9, '#6E7B76')
    c.text(x + w - 16, y + 15, '11-08  19:30', 9, '#52625C', mono=True, align='right')
    c.text(x + 16, y + 51, '那不勒斯', 21, '#152B27', bold=True)
    c.text(x + 127, y + 58, 'VS', 9, '#9AA49F', mono=True)
    c.text(x + 161, y + 51, '拉齐奥', 21, '#152B27', bold=True)
    c.text(x + 16, y + 96, '最高概率倾向', 9, '#6E7B76')
    c.text(x + w - 16, y + 88, '主胜  42.6%', 20, '#1F664F', bold=True, mono=True, align='right')
    probability_bar(c, x + 16, y + 128, w - 32, [42.6, 28.0, 29.4], ['#2C765D', '#C8D0CB', '#84968D'], h=8)
    cols = [('主胜', '42.6%', '#1F664F'), ('平局', '28.0%', '#344A42'), ('客胜', '29.4%', '#344A42')]
    for i, (label, value, color) in enumerate(cols):
        xx = x + 16 + i * 109
        if i:
            c.line([(xx - 10, y + 151), (xx - 10, y + 189)], '#E1E5E2', 1)
        c.text(xx, y + 151, label, 9, '#7B8782')
        c.text(xx, y + 170, value, 13, color, bold=True, mono=True)
    c.rect(x + 16, y + 205, w - 32, 1, '#E3E7E4')
    c.text(x + 16, y + 220, '参考比分', 9, '#6E7B76')
    c.text(x + 88, y + 218, '1:1  13.3%', 10, '#263D35', mono=True)
    c.text(x + 185, y + 218, '1:0  10.8%', 10, '#263D35', mono=True)
    c.rr(x + 16, y + 250, 118, 42, 8, '#F4F6F3')
    c.text(x + 26, y + 258, '数据完整度', 8, '#6E7B76')
    c.text(x + 26, y + 274, '20%', 12, '#9A6424', bold=True, mono=True)
    c.rr(x + 143, y + 250, 199, 42, 8, '#FBF4E8')
    c.text(x + 153, y + 258, '方法', 8, '#8D7555')
    c.text(x + 153, y + 274, '赔率降级估计 · 弱参考', 9, '#8A5B24', bold=True)
    c.text(x + 16, y + 301, '风险：缺失 home_xg 等 7 项', 8.5, '#9A6B35')


def overview_b():
    c = Canvas(390, 844, 2.4, '#F4F5F1')
    light_header(c)
    c.text(16, 86, '工程信息中心 / 2026.11.08', 9, '#2C765D', bold=True)
    c.text(16, 116, '让每一次推断，', 29, '#152B27', bold=True)
    c.text(16, 156, '都有迹可循。', 29, '#2C765D', bold=True)
    c.para(16, 204, '从官方对阵到赛后回测，一条可复现、可审计的数据流水线。', 354, 12, '#687771', 20)
    c.rr(16, 258, 358, 128, 12, '#FFFFFF', '#D9DEDA')
    c.text(32, 274, '今日概率报告', 11, '#687771', bold=True)
    c.text(32, 298, '25', 37, '#152B27', bold=True, mono=True)
    c.text(82, 315, '场比赛 / 25 条策略', 11, '#52625C')
    c.pill(284, 276, '已生成', 9, '#2C765D', '#E4F0E9', h=24)
    c.rr(32, 344, 326, 30, 7, '#163C34')
    c.text(48, 351, '查看每日推断', 11, '#FFFFFF', bold=True)
    arrow(c, 335, 359, '#FFFFFF', 8)
    c.rr(16, 398, 173, 42, 8, '#FFFFFF', '#CBD4CE')
    c.text(102, 410, '探索数据流水线', 11, '#314A42', align='center')
    c.rr(201, 398, 173, 42, 8, '#FFFFFF', '#CBD4CE')
    c.text(287, 410, '复制每日命令', 11, '#314A42', align='center')
    c.text(16, 466, '工程概览', 15, '#152B27', bold=True)
    stats = [('04', '每日报告格式'), ('08', '核心数据表'), ('05', '前列比分输出'), ('04', 'AI 策略分类')]
    for i, (value, label) in enumerate(stats):
        xx = 16 + (i % 2) * 182
        yy = 500 + (i // 2) * 82
        c.rr(xx, yy, 176, 70, 10, '#FFFFFF', '#DDE2DE')
        c.text(xx + 14, yy + 10, value, 22, '#2C765D', bold=True, mono=True)
        c.text(xx + 59, yy + 18, label, 10, '#3E514A', bold=True)
        c.text(xx + 14, yy + 45, '可复现 / 可回溯', 8, '#8A9691')
    c.text(16, 675, '四条核心设计原则', 14, '#152B27', bold=True)
    c.text(16, 705, '01  官方对阵    02  版本追加', 10, '#61716A')
    c.text(16, 731, '03  时间边界    04  显式中止', 10, '#61716A')
    bottom_nav(c, 0, '#FFFFFF', '#E3EFEA', '#87928D', '#2C765D', '#E0E5E1', dark=False)
    return c


def daily_b():
    c = Canvas(390, 844, 2.4, '#F4F5F1')
    back(c, 16, 33, '#2C765D')
    c.text(45, 21, '每日推断', 18, '#152B27', bold=True)
    c.text(374, 26, '2026.11.08', 10, '#687771', mono=True, align='right')
    c.text(16, 67, '25 场 / 25 条策略', 11, '#41564E', bold=True)
    c.pill(282, 60, 'Python 每日打包', 9, '#52675E', '#E7ECE8', h=27)
    c.rr(16, 105, 358, 61, 10, '#FFF9EF', '#E9D8B9')
    c.circle(30, 120, 18, '#A87435')
    c.text(39, 122, 'i', 10, '#FFFFFF', bold=True, align='center')
    c.para(58, 117, '概率估计仅供分析，不构成收益保证。低完整度仅作弱参考。', 301, 10, '#80643E', 17)
    c.text(16, 190, '比赛报告', 15, '#152B27', bold=True)
    c.text(374, 194, '概率 / 比分 / 完整度 / 风险', 8.5, '#7B8782', align='right')
    match_card_b(c, 224)
    c.rr(16, 556, 358, 185, 12, '#FFFFFF', '#D9DEDA')
    c.rect(16, 556, 4, 185, '#607F74')
    c.text(32, 572, '周日007', 10, '#2C765D', bold=True, mono=True)
    c.text(103, 572, '西班牙甲级联赛', 9, '#6E7B76')
    c.text(358, 571, '21:00', 9, '#52625C', mono=True, align='right')
    c.text(32, 607, '埃尔切', 20, '#152B27', bold=True)
    c.text(123, 614, 'VS', 9, '#9AA49F', mono=True)
    c.text(157, 607, '皇家贝蒂斯', 20, '#152B27', bold=True)
    c.text(32, 651, '最高概率倾向', 9, '#6E7B76')
    c.text(358, 643, '客胜  55.7%', 19, '#1F664F', bold=True, mono=True, align='right')
    probability_bar(c, 32, 682, 326, [18.7, 25.6, 55.7], ['#84968D', '#C8D0CB', '#2C765D'], h=8)
    c.text(32, 705, '完整度 20%', 9, '#9A6424', bold=True)
    c.text(358, 705, '赔率降级估计 · 弱参考', 9, '#9A6424', align='right')
    return c


def pitch_lines(c, color):
    c.rr(210, 88, 220, 148, 12, None, color, 1)
    c.line([(320, 88), (320, 236)], color, 1)
    c.draw.ellipse(c.box(278, 128, 84, 68), outline=color, width=c.p(1))
    c.rect(210, 125, 34, 74, None, color, 1)
    c.rect(396, 125, 34, 74, None, color, 1)


def team_badge(c, x, y, char, color, bg):
    c.circle(x, y, 42, bg, color, 1.2)
    c.text(x + 21, y + 10, char, 17, color, bold=True, align='center')


def overview_c():
    t = {'accent': '#B9F54A', 'accent_text': '#10200B', 'text': '#F4FAEC', 'muted': '#9AAF94'}
    c = Canvas(390, 844, 2.4, '#123B25')
    gradient(c, '#164D2C', '#0B2518')
    pitch_lines(c, '#2B6740')
    header_brand(c, t, 'FOOTBALL INTELLIGENCE')
    c.pill(16, 88, 'MATCHDAY / 2026.11.08', 8, '#C8F99A', '#235D36', h=25, mono=True)
    c.text(16, 132, '让每一次推断，', 30, '#F4FAEC', bold=True)
    c.text(16, 173, '都有迹可循。', 30, '#B9F54A', bold=True)
    c.para(16, 222, '看见概率，也看见不确定性。', 260, 13, '#C2D3BC', 20, bold=True)
    c.rr(16, 273, 358, 52, 15, '#B9F54A')
    c.text(34, 288, '进入今日比赛中心', 13, '#10200B', bold=True)
    c.text(268, 286, '25 场', 12, '#254215', bold=True, mono=True)
    arrow(c, 348, 299, '#10200B', 9)
    c.rr(16, 340, 358, 194, 20, '#12351F', '#3C7144')
    c.text(32, 357, 'SCORE MATRIX', 8, '#91AD8C', mono=True)
    c.text(32, 378, '比分概率矩阵', 15, '#F4FAEC', bold=True)
    c.pill(279, 356, '模型 V1', 9, '#B9F54A', '#214B2A', h=24, mono=True)
    matrix(c, 32, 414, 230, 82, '#B9F54A', '#2C5A32', rounded=3)
    c.text(285, 419, 'P', 17, '#B9F54A', bold=True, mono=True)
    c.text(285, 444, '完整分布', 9, '#C2D3BC')
    c.text(285, 470, 'V1', 13, '#B9F54A', bold=True, mono=True)
    c.text(285, 491, '版本追加', 9, '#C2D3BC')
    stats = [('04', '报告格式'), ('08', '数据表'), ('05', '比分输出'), ('04', '策略分类')]
    for i, (value, label) in enumerate(stats):
        xx = 16 + i * 90
        c.text(xx, 559, value, 24, '#F4FAEC', bold=True, mono=True)
        c.text(xx, 594, label, 9, '#9AAF94')
    c.rr(16, 636, 358, 101, 16, '#153C23', '#3A6A40')
    c.text(32, 652, '今天，也从边界开始', 14, '#F4FAEC', bold=True)
    c.para(32, 680, '官方对阵 · 赛前信息 · 追加版本 · 赛后回测', 310, 10, '#B5C9AE', 18)
    c.pill(32, 710, '概率分析，而非收益承诺', 9, '#D6E7B7', '#284C27', h=23)
    bottom_nav(c, 0, '#0D2B1A', '#2B5527', '#8FA58A', '#B9F54A', '#315A35')
    return c


def daily_c():
    c = Canvas(390, 844, 2.4, '#0F321F')
    gradient(c, '#164D2C', '#0B2518')
    back(c, 16, 33, '#B9F54A')
    c.text(45, 21, '今日比赛中心', 18, '#F4FAEC', bold=True)
    c.pill(292, 20, '11 月 08 日', 9, '#D9EBCF', '#255B35', h=28)
    c.text(16, 68, '25 场 / 25 条策略', 11, '#C2D3BC', bold=True)
    c.rr(16, 102, 358, 55, 13, '#1B4628', '#4C7040')
    c.text(30, 114, '概率估计仅供分析', 10, '#D9E7CE', bold=True)
    c.text(30, 135, '低完整度仅作弱参考，不构成收益保证。', 9, '#B6C9A9')
    c.rr(16, 177, 358, 348, 20, '#12351F', '#467847')
    c.pill(30, 191, '周日006', 9, '#B9F54A', '#244E29', h=24, bold=True, mono=True)
    c.text(104, 196, '意甲 · 19:30', 9, '#AFC5A8')
    c.text(360, 195, 'KIMI', 8, '#91AD8C', mono=True, align='right')
    team_badge(c, 58, 235, '那', '#B9F54A', '#214B2A')
    team_badge(c, 290, 235, '拉', '#B9F54A', '#214B2A')
    c.text(79, 287, '那不勒斯', 14, '#F4FAEC', bold=True, align='center')
    c.text(311, 287, '拉齐奥', 14, '#F4FAEC', bold=True, align='center')
    c.text(195, 240, 'VS', 16, '#769773', bold=True, mono=True, align='center')
    c.text(195, 267, '最高倾向', 8, '#91AD8C', align='center')
    c.pill(155, 287, '主胜', 11, '#10200B', '#B9F54A', h=28, bold=True)
    c.text(30, 332, '主胜  42.6%', 22, '#B9F54A', bold=True, mono=True)
    c.text(360, 339, '平 28.0%  /  客 29.4%', 10, '#C2D3BC', mono=True, align='right')
    probability_bar(c, 30, 372, 330, [42.6, 28.0, 29.4], ['#B9F54A', '#4C6C4C', '#80967B'], h=9)
    c.text(30, 400, '参考比分', 9, '#91AD8C')
    c.pill(93, 392, '1:1  13.3%', 9, '#F0F6E9', '#22472A', h=28, mono=True)
    c.pill(193, 392, '1:0  10.8%', 9, '#F0F6E9', '#22472A', h=28, mono=True)
    c.line([(30, 438), (360, 438)], '#315D37', 1)
    completeness(c, 30, 455, 20, '#E8BD67', '#365337', '#D5DFCE')
    c.pill(139, 449, '赔率降级估计 · 弱参考', 9, '#E9C780', '#463D24', h=27)
    c.text(30, 497, '!  缺失 home_xg 等 7 项风险', 9, '#D8B978')
    c.rr(16, 541, 358, 199, 20, '#12351F', '#3B693E')
    c.pill(30, 555, '周日007', 9, '#B9F54A', '#244E29', h=24, bold=True, mono=True)
    c.text(104, 560, '西甲 · 21:00', 9, '#AFC5A8')
    c.text(30, 599, '埃尔切', 19, '#F4FAEC', bold=True)
    c.text(123, 605, 'VS', 9, '#769773', mono=True)
    c.text(157, 599, '皇家贝蒂斯', 19, '#F4FAEC', bold=True)
    c.text(30, 645, '客胜', 13, '#B9F54A', bold=True)
    c.text(360, 638, '55.7%', 24, '#B9F54A', bold=True, mono=True, align='right')
    probability_bar(c, 30, 681, 330, [18.7, 25.6, 55.7], ['#80967B', '#4C6C4C', '#B9F54A'], h=9)
    c.text(30, 710, '完整度 20%  ·  弱参考', 9, '#D8B978')
    return c


def mini_status(c, x, y, label, value, accent):
    c.text(x, y, value, 20, '#F4F1EA', bold=True, mono=True)
    c.text(x, y + 30, label, 8.5, '#8C8B93')
    c.rect(x, y + 50, 54, 2, accent)


def compact_match_d(c, y, code, league, home, away, time, tendency, prob, values, expanded=False):
    x, w = 16, 358
    h = 274 if expanded else 118
    c.rr(x, y, w, h, 8, '#15161D', '#30313B')
    c.rect(x, y, 3, h, '#FF7A45' if expanded else '#4A4B55')
    c.text(x + 14, y + 14, code, 9, '#FF9A70', bold=True, mono=True)
    c.text(x + 80, y + 14, league, 8.5, '#92919B')
    c.text(x + w - 14, y + 13, time, 9, '#C4C2C9', mono=True, align='right')
    c.text(x + 14, y + 44, home, 16 if expanded else 14, '#F4F1EA', bold=True)
    c.text(x + 14, y + 70 if expanded else y + 67, away, 16 if expanded else 14, '#F4F1EA', bold=True)
    c.text(x + w - 95, y + 45, tendency, 10, '#FF9A70', bold=True, align='right')
    c.text(x + w - 14, y + 40, prob, 23, '#FF7A45', bold=True, mono=True, align='right')
    if expanded:
        probability_bar(c, x + 14, y + 105, w - 28, values, ['#FF7A45', '#575761', '#8A8991'], h=6)
        for i, (label, value) in enumerate(zip(['主胜', '平局', '客胜'], ['42.6', '28.0', '29.4'])):
            xx = x + 14 + i * 111
            c.text(xx, y + 124, label, 8, '#85848D')
            c.text(xx + 38, y + 121, value + '%', 11, '#E9E6E2', bold=True, mono=True)
        c.line([(x + 14, y + 153), (x + w - 14, y + 153)], '#30313B', 1)
        c.text(x + 14, y + 168, 'SCORE', 8, '#85848D', mono=True)
        c.text(x + 72, y + 165, '1:1 / 13.3%   1:0 / 10.8%', 10, '#E9E6E2', mono=True)
        c.text(x + 14, y + 199, 'DATA', 8, '#85848D', mono=True)
        c.text(x + 72, y + 197, '完整度 20%', 10, '#E8B86D', bold=True)
        c.text(x + w - 14, y + 197, '赔率降级估计 · 弱参考', 9, '#E8B86D', align='right')
        c.rr(x + 14, y + 229, w - 28, 30, 5, '#2A231F')
        c.text(x + 24, y + 237, 'RISK  缺失 home_xg 等 7 项', 8.5, '#D5A66A', mono=False)
    else:
        probability_bar(c, x + 14, y + 94, w - 28, values, ['#FF7A45', '#575761', '#8A8991'], h=5)


def overview_d():
    t = {'accent': '#FF7A45', 'accent_text': '#1C100B', 'text': '#F4F1EA', 'muted': '#8C8B93'}
    c = Canvas(390, 844, 2.4, '#0D0E13')
    c.rect(0, 0, 390, 5, '#FF7A45')
    c.text(16, 22, 'FM // RESEARCH LOG', 12, '#F4F1EA', bold=True, mono=True)
    c.text(374, 24, 'BUILD 01', 8, '#8C8B93', mono=True, align='right')
    c.line([(16, 58), (374, 58)], '#34353E', 1)
    c.text(16, 82, '足球经理的推断', 22, '#F4F1EA', bold=True)
    c.text(16, 119, '让每一次推断，都有迹可循。', 13, '#FF9A70', bold=True)
    c.para(16, 151, '官方对阵 / 赛前特征 / 概率模型 / 回测闭环', 350, 10, '#9C9BA4', 17, mono=False)
    c.text(16, 198, 'TODAY', 9, '#8C8B93', mono=True)
    c.text(16, 219, '25', 58, '#F4F1EA', bold=True, mono=True)
    c.text(105, 249, '场比赛', 13, '#C4C2C9', bold=True)
    c.text(245, 222, '25 条策略', 12, '#F4F1EA', bold=True)
    c.text(245, 248, '2026.11.08', 10, '#8C8B93', mono=True)
    c.rr(16, 303, 358, 48, 7, '#FF7A45')
    c.text(32, 316, '进入每日推断', 12, '#1C100B', bold=True)
    c.text(343, 316, '[ → ]', 12, '#1C100B', bold=True, mono=True, align='right')
    c.rr(16, 363, 173, 40, 7, '#15161D', '#393A44')
    c.text(102, 374, '数据流水线', 10, '#C4C2C9', align='center')
    c.rr(201, 363, 173, 40, 7, '#15161D', '#393A44')
    c.text(287, 374, '复制每日命令', 10, '#C4C2C9', align='center')
    c.text(16, 432, 'PIPELINE STATUS', 9, '#8C8B93', mono=True)
    nodes = ['对阵', '快照', '特征', '推断', '报告', '赛果', '回测']
    for i, label in enumerate(nodes):
        xx = 23 + i * 52
        if i < 6:
            c.line([(xx + 9, 469), (xx + 52, 469)], '#44454E', 2)
        c.circle(xx, 461, 16, '#FF7A45' if i < 5 else '#26272F', '#FF7A45' if i < 5 else '#565760')
        c.text(xx + 8, 464, str(i + 1), 7, '#1C100B' if i < 5 else '#9C9BA4', bold=True, mono=True, align='center')
        c.text(xx + 8, 487, label, 8, '#B6B4BC', align='center')
    c.line([(16, 521), (374, 521)], '#34353E', 1)
    mini_status(c, 16, 545, '报告格式', '04', '#FF7A45')
    mini_status(c, 106, 545, '核心数据表', '08', '#5C5D66')
    mini_status(c, 196, 545, '前列比分', '05', '#5C5D66')
    mini_status(c, 286, 545, '策略分类', '04', '#5C5D66')
    c.rr(16, 634, 358, 105, 7, '#15161D', '#30313B')
    c.text(30, 649, 'SYSTEM PRINCIPLES', 8, '#8C8B93', mono=True)
    c.text(30, 673, '01 官方对阵   02 版本追加', 10, '#D6D3D2', mono=False)
    c.text(30, 699, '03 时间边界   04 显式中止', 10, '#D6D3D2', mono=False)
    bottom_nav(c, 0, '#111218', '#38261F', '#777680', '#FF7A45', '#30313B')
    return c


def daily_d():
    c = Canvas(390, 844, 2.4, '#0D0E13')
    c.rect(0, 0, 390, 5, '#FF7A45')
    back(c, 16, 32, '#FF7A45')
    c.text(44, 19, 'DAILY / 每日推断', 16, '#F4F1EA', bold=True, mono=False)
    c.text(374, 24, '2026.11.08', 9, '#9C9BA4', mono=True, align='right')
    c.text(16, 62, '25 MATCHES', 20, '#F4F1EA', bold=True, mono=True)
    c.text(174, 68, '/ 25 STRATEGIES', 9, '#8C8B93', mono=True)
    c.rr(16, 105, 358, 52, 6, '#211B18', '#50372A')
    c.text(28, 116, 'NOTICE', 8, '#FF9A70', bold=True, mono=True)
    c.para(88, 113, '概率估计不构成收益保证；低完整度仅作弱参考。', 272, 9, '#C5A992', 15)
    c.text(16, 178, 'MATCH', 8, '#777680', mono=True)
    c.text(374, 178, 'TENDENCY / PROBABILITY', 8, '#777680', mono=True, align='right')
    compact_match_d(c, 202, '周日006', '意大利甲级联赛', '那不勒斯', '拉齐奥', '19:30', '主胜', '42.6%', [42.6, 28.0, 29.4], True)
    compact_match_d(c, 490, '周日007', '西班牙甲级联赛', '埃尔切', '皇家贝蒂斯', '21:00', '客胜', '55.7%', [18.7, 25.6, 55.7])
    compact_match_d(c, 622, '周日009', '荷兰甲级联赛', '特温特', '阿贾克斯', '21:30', '主胜', '45.9%', [45.9, 27.6, 26.5])
    c.text(16, 765, '完整列表纵向滚动  ↓', 9, '#8C8B93')
    c.text(374, 765, '概率 / 完整度 / 风险同时保留', 8, '#8C8B93', align='right')
    return c


THEMES = [
    {
        'id': 'A', 'slug': 'dark-data-cockpit', 'name': '深色数据舱',
        'subtitle': '保留现有品牌识别，用更清晰的数据层级降低阅读成本',
        'overview': overview_a, 'daily': daily_a,
        'accent': '#A4C94E',
        'keywords': ['深色专业', '概率优先', '低风险提示'],
        'changes': [
            '首屏主按钮直接承接「25 场每日推断」',
            '三向概率条与最高倾向双重编码，扫读更快',
            '完整度、弱参考与风险并列，不再藏在卡片末尾',
            '模型矩阵退为辅助证据，减少首屏视觉争抢',
        ],
        'best': '希望保留现在的深绿品牌，同时尽快升级 H5 与小游戏。',
        'cost': '低', 'cost_n': 1,
        'palette': ['#0E141B', '#151C24', '#C9F66F', '#82929E', '#E8B85D'],
    },
    {
        'id': 'B', 'slug': 'light-editorial-report', 'name': '浅色专业报表',
        'subtitle': '以长列表阅读和截图分享为核心，像一份清晰的研究报告',
        'overview': overview_b, 'daily': daily_b,
        'accent': '#2C765D',
        'keywords': ['浅色高对比', '表格化', '报告阅读'],
        'changes': [
            '今日报告成为首屏主角，营销文案适当收短',
            '白底卡片与细分隔线适合连续阅读 25 场比赛',
            '概率数字采用等宽字体，纵向比较更稳定',
            '风险使用低饱和琥珀色，不依赖刺眼红色',
        ],
        'best': '偏重报告阅读、截图转发，或希望整体更轻盈、克制。',
        'cost': '中', 'cost_n': 2,
        'palette': ['#F4F5F1', '#FFFFFF', '#163C34', '#2C765D', '#A87435'],
    },
    {
        'id': 'C', 'slug': 'stadium-scoreboard', 'name': '球场运动风',
        'subtitle': '用球场线、队名徽记与记分牌层级增强赛事氛围',
        'overview': overview_c, 'daily': daily_c,
        'accent': '#76A83B',
        'keywords': ['赛事氛围', '记分牌', '品牌感强'],
        'changes': [
            '概览加入轻量球场线，强化足球产品识别',
            '比赛卡转为记分牌结构，双方与倾向更聚焦',
            '最高概率成为视觉锚点，其余两项仍完整展示',
            '装饰仅使用 Canvas 图形，无需新增图片资产',
        ],
        'best': '希望产品更有运动感和记忆点，而不只是工程信息中心。',
        'cost': '中高', 'cost_n': 3,
        'palette': ['#0B2518', '#164D2C', '#B9F54A', '#C2D3BC', '#E8BD67'],
    },
    {
        'id': 'D', 'slug': 'compact-match-intel', 'name': '紧凑赛事情报',
        'subtitle': '以列表扫描效率为先，适合比赛多、打开频率高的使用方式',
        'overview': overview_d, 'daily': daily_d,
        'accent': '#D66A3E',
        'keywords': ['高信息密度', '终端感', '快速扫描'],
        'changes': [
            '首屏直接展示「25 场」与流水线状态，路径更短',
            '首场显示完整信息，后续比赛采用紧凑行卡片',
            '时间、队名、倾向和最高概率形成固定扫描列',
            '橙色只标记关键数据，风险仍独立使用琥珀色',
        ],
        'best': '经常快速浏览整天对阵，希望减少滚动与重复卡片。',
        'cost': '中', 'cost_n': 2,
        'palette': ['#0D0E13', '#15161D', '#FF7A45', '#8C8B93', '#E8B86D'],
    },
]


def rounded_phone_image(screen):
    img = screen.image
    mask = Image.new('L', img.size, 0)
    md = ImageDraw.Draw(mask)
    md.rounded_rectangle((0, 0, img.width - 1, img.height - 1), radius=round(24 * screen.s), fill=255)
    out = Image.new('RGBA', img.size, (0, 0, 0, 0))
    out.paste(img, (0, 0), mask)
    return out


def add_phone(board, screen, x, y):
    phone = rounded_phone_image(screen)
    shadow = Image.new('RGBA', board.image.size, (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    sx, sy = board.p(x), board.p(y)
    sd.rounded_rectangle((sx - board.p(2), sy + board.p(8), sx + phone.width + board.p(2), sy + phone.height + board.p(12)), radius=board.p(28), fill=(20, 25, 30, 65))
    shadow = shadow.filter(ImageFilter.GaussianBlur(board.p(14)))
    board.image.paste(shadow, (0, 0), shadow)
    board.draw = ImageDraw.Draw(board.image)
    board.image.paste(phone, (sx, sy), phone)
    board.draw = ImageDraw.Draw(board.image)
    board.draw.rounded_rectangle((sx, sy, sx + phone.width - 1, sy + phone.height - 1), radius=board.p(24), outline='#CBD0D4', width=board.p(1))


def render_board(theme):
    b = Canvas(2000, 1400, 2, '#F2F3F5')
    b.text(72, 46, 'FOOTBALL MANAGER  /  UI CONCEPTS 2026', 16, '#737982', mono=True)
    b.text(72, 84, f"{theme['id']}｜{theme['name']}", 46, '#171A1F', bold=True)
    b.text(72, 150, theme['subtitle'], 21, '#60666F')
    b.text(72, 196, '01  概览', 16, '#4C535C', bold=True)
    b.text(592, 196, '02  每日推断', 16, '#4C535C', bold=True)
    overview = theme['overview']()
    daily = theme['daily']()
    add_phone(b, overview, 72, 228)
    add_phone(b, daily, 592, 228)

    x = 1130
    b.rect(x, 228, 6, 68, theme['accent'])
    b.text(x + 28, 228, '设计关键词', 18, '#737982', bold=True)
    cx = x + 28
    for word in theme['keywords']:
        w = b.width(word, 18, True) + 32
        b.rr(cx, 270, w, 38, 19, '#FFFFFF', '#D8DCE0')
        b.text(cx + w / 2, 279, word, 18, '#3F464E', bold=True, align='center')
        cx += w + 12

    b.text(x, 355, '核心改动', 27, '#1E2228', bold=True)
    yy = 410
    for i, change in enumerate(theme['changes'], 1):
        b.circle(x + 2, yy + 2, 28, '#FFFFFF', '#D8DCE0')
        b.text(x + 16, yy + 7, f'{i:02d}', 13, theme['accent'], bold=True, mono=True, align='center')
        yy = b.para(x + 48, yy, change, 720, 21, '#434A53', 34) + 24

    b.line([(x, 760), (1880, 760)], '#D7DBDF', 1)
    b.text(x, 798, '最适合', 20, '#737982', bold=True)
    b.para(x, 840, theme['best'], 750, 23, '#252A31', 38, bold=True)

    b.text(x, 956, '落地成本', 20, '#737982', bold=True)
    for i in range(3):
        b.rr(x + 120 + i * 58, 960, 44, 12, 6, theme['accent'] if i < theme['cost_n'] else '#D9DDE1')
    b.text(x + 310, 949, theme['cost'], 24, '#252A31', bold=True)
    cost_note = ['主题与组件层级调整为主', '需要双端主题与布局适配', '需要较多定制绘制与细节验收'][theme['cost_n'] - 1]
    b.text(x + 380, 958, cost_note, 17, '#737982')

    b.text(x, 1050, '核心配色', 20, '#737982', bold=True)
    for i, color in enumerate(theme['palette']):
        xx = x + i * 142
        b.rr(xx, 1094, 112, 68, 12, color, '#D0D4D8')
        b.text(xx, 1178, color, 15, '#666D76', mono=True)

    b.text(72, 1320, '概念稿使用 2026-11-08 报告快照；保留概率、完整度、风险与免责声明。仅用于视觉选型，未修改业务代码。', 16, '#7A8088')
    b.text(1928, 1320, f"OPTION {theme['id']}", 16, theme['accent'], bold=True, mono=True, align='right')

    path = OUT / f"{theme['id']}-{theme['slug']}.png"
    final = b.image.resize((2000, 1400), Image.Resampling.LANCZOS)
    final.save(path, optimize=True)
    # Separate screen pair is useful for closer inspection and later implementation reference.
    pair = Image.new('RGB', (overview.image.width + daily.image.width + 40, max(overview.image.height, daily.image.height)), '#E5E7E9')
    pair.paste(overview.image, (0, 0))
    pair.paste(daily.image, (overview.image.width + 40, 0))
    pair.save(OUT / f"{theme['id']}-screens.png", optimize=True)
    return path


def render_contact_sheet():
    board = Canvas(2200, 1650, 1.5, '#F2F3F5')
    board.text(80, 58, 'FOOTBALL MANAGER  /  UI DIRECTIONS', 18, '#737982', mono=True)
    board.text(80, 102, '四套界面方向总览', 48, '#171A1F', bold=True)
    board.text(80, 172, '同一批真实数据 · 同一对页面 · 四种信息层级与品牌表达', 22, '#646B74')
    positions = [(80, 250), (1120, 250), (80, 930), (1120, 930)]
    for theme, (x, y) in zip(THEMES, positions):
        board.rr(x, y, 1000, 590, 24, '#FFFFFF', '#D9DDE1')
        board.rect(x, y, 8, 590, theme['accent'])
        board.text(x + 36, y + 30, f"{theme['id']}｜{theme['name']}", 32, '#1B2026', bold=True)
        board.text(x + 36, y + 80, ' / '.join(theme['keywords']), 18, '#6B727B')
        screen = theme['overview']()
        thumb = screen.image.resize((230, 498), Image.Resampling.LANCZOS)
        board.image.paste(thumb, (board.p(x + 36), board.p(y + 130)))
        board.draw = ImageDraw.Draw(board.image)
        daily = theme['daily']()
        thumb2 = daily.image.resize((230, 498), Image.Resampling.LANCZOS)
        board.image.paste(thumb2, (board.p(x + 282), board.p(y + 130)))
        board.draw = ImageDraw.Draw(board.image)
        board.text(x + 550, y + 140, '设计重点', 20, '#777E87', bold=True)
        yy = y + 190
        for change in theme['changes'][:3]:
            board.circle(x + 552, yy + 6, 8, theme['accent'])
            yy = board.para(x + 574, yy, change, 380, 18, '#414851', 30) + 22
        board.text(x + 550, y + 490, f"落地成本：{theme['cost']}", 20, '#252B32', bold=True)
        for i, color in enumerate(theme['palette']):
            board.rr(x + 550 + i * 70, y + 538, 54, 22, 6, color)
    final = board.image.resize((2200, 1650), Image.Resampling.LANCZOS)
    final.save(OUT / '00-four-directions.png', optimize=True)


if __name__ == '__main__':
    OUT.mkdir(parents=True, exist_ok=True)
    for theme in THEMES:
        path = render_board(theme)
        print(path.name)
    render_contact_sheet()
    print('00-four-directions.png')
