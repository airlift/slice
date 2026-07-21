# C++ Benchmark Deviation Report: O0 vs O3

> Historical record. This document is evidence, not current project status.
> See the repository-root `RE2_TASKS.md` for open work.

Comparison: `cpp-baseline-002.csv` (O0, no optimization) vs `cpp-baseline-003.csv` (O3, Release)

Shows the impact of enabling `-O3 -DNDEBUG` compiler optimization.

## All Comparisons

| Category | Benchmark | Engine | Size | O0 (ns) | O3 (ns) | Speedup |
|----------|-----------|--------|------|---------|---------|---------|
| Search | Parens | NFA | 256K | 213,441,000 | 7,387,310 | 28.9x |
| Search | Parens | NFA | 32K | 26,542,000 | 934,741 | 28.4x |
| Search | Parens | NFA | 512 | 418,188 | 14,797 | 28.3x |
| Search | Parens | NFA | 4K | 3,322,300 | 118,281 | 28.1x |
| Search | Parens | NFA | 64 | 54,116 | 2,056 | 26.3x |
| Search | Hard | NFA | 4K | 1,538,800 | 80,703 | 19.1x |
| Search | Hard | NFA | 32K | 12,277,900 | 647,763 | 19.0x |
| Search | Hard | NFA | 256K | 98,116,600 | 5,195,620 | 18.9x |
| Search | Hard | NFA | 512 | 194,516 | 10,425 | 18.7x |
| Search | Parens | NFA | 8 | 8,539 | 458.0 | 18.6x |
| Search | Hard | NFA | 64 | 25,688 | 1,388 | 18.5x |
| Parse | Split | NFA | 12 | 9,759 | 538.0 | 18.1x |
| Parse | DigitDs | NFA | 12 | 9,272 | 512.0 | 18.1x |
| Parse | Digits | NFA | 12 | 9,271 | 517.0 | 17.9x |
| Compile | CompileToProg | Compile | 0 | 43,183 | 2,477 | 17.4x |
| Search | Easy1 | NFA | 4K | 17,446 | 1,180 | 14.8x |
| Compile | RE2_Compile | RE2 | 0 | 52,753 | 3,667 | 14.4x |
| Search | Easy1 | NFA | 32K | 158,254 | 11,432 | 13.8x |
| Compile | Regexp_SimplifyCompile | SimplifyCompile | 0 | 47,994 | 3,518 | 13.6x |
| Search | Medium | NFA | 4K | 783,125 | 60,130 | 13.0x |
| Search | Medium | NFA | 256K | 49,980,900 | 3,853,870 | 13.0x |
| Search | Medium | NFA | 512 | 100,394 | 7,752 | 13.0x |
| Search | Hard | NFA | 8 | 4,516 | 349.0 | 12.9x |
| Search | Medium | NFA | 32K | 6,281,430 | 492,319 | 12.8x |
| Search | Easy1 | NFA | 256K | 1,159,170 | 92,076 | 12.6x |
| Search | Medium | NFA | 64 | 13,777 | 1,145 | 12.0x |
| Parse | Digits | Backtrack | 12 | 2,264 | 199.0 | 11.4x |
| Search | Easy1 | NFA | 512 | 4,005 | 353.0 | 11.3x |
| Search | Hard | RE2 | 4K | 191.0 | 17.0 | 11.2x |
| Search | Medium | RE2 | 16M | 191.0 | 17.0 | 11.2x |
| Parse | DigitDs | Backtrack | 12 | 2,259 | 202.0 | 11.2x |
| Search | Hard | RE2 | 64 | 190.0 | 17.0 | 11.2x |
| Search | Hard | RE2 | 512 | 190.0 | 17.0 | 11.2x |
| Search | Hard | RE2 | 256K | 190.0 | 17.0 | 11.2x |
| Search | Medium | RE2 | 4K | 190.0 | 17.0 | 11.2x |
| Search | Medium | RE2 | 32K | 190.0 | 17.0 | 11.2x |
| Search | Parens | RE2 | 4K | 190.0 | 17.0 | 11.2x |
| Search | Parens | RE2 | 256K | 190.0 | 17.0 | 11.2x |
| Search | Parens | RE2 | 2M | 190.0 | 17.0 | 11.2x |
| Search | Easy1 | RE2 | 2M | 189.0 | 17.0 | 11.1x |
| Search | Hard | RE2 | 8 | 189.0 | 17.0 | 11.1x |
| Search | Parens | RE2 | 32K | 189.0 | 17.0 | 11.1x |
| Search | Easy0 | RE2 | 16M | 192.0 | 18.0 | 10.7x |
| Search | Parens | RE2 | 64 | 192.0 | 18.0 | 10.7x |
| Search | Easy1 | RE2 | 16M | 191.0 | 18.0 | 10.6x |
| Search | Hard | RE2 | 2M | 191.0 | 18.0 | 10.6x |
| Search | Medium | RE2 | 8 | 191.0 | 18.0 | 10.6x |
| Search | Medium | RE2 | 64 | 191.0 | 18.0 | 10.6x |
| Search | Medium | RE2 | 256K | 191.0 | 18.0 | 10.6x |
| Search | Easy0 | RE2 | 256K | 190.0 | 18.0 | 10.6x |
| Search | Hard | RE2 | 32K | 190.0 | 18.0 | 10.6x |
| Search | Hard | RE2 | 16M | 190.0 | 18.0 | 10.6x |
| Search | Medium | RE2 | 512 | 190.0 | 18.0 | 10.6x |
| Search | Medium | RE2 | 2M | 190.0 | 18.0 | 10.6x |
| Search | Parens | RE2 | 8 | 190.0 | 18.0 | 10.6x |
| Search | Parens | RE2 | 512 | 190.0 | 18.0 | 10.6x |
| Search | Parens | RE2 | 16M | 190.0 | 18.0 | 10.6x |
| Search | Easy0 | RE2 | 512 | 189.0 | 18.0 | 10.5x |
| Search | Easy0 | RE2 | 4K | 189.0 | 18.0 | 10.5x |
| Search | Easy0 | RE2 | 2M | 189.0 | 18.0 | 10.5x |
| Search | Easy1 | RE2 | 64 | 189.0 | 18.0 | 10.5x |
| Search | Easy1 | RE2 | 512 | 189.0 | 18.0 | 10.5x |
| Search | Easy1 | RE2 | 4K | 189.0 | 18.0 | 10.5x |
| Search | Easy1 | RE2 | 32K | 189.0 | 18.0 | 10.5x |
| Search | Easy1 | RE2 | 256K | 189.0 | 18.0 | 10.5x |
| Search | Medium | NFA | 8 | 3,065 | 293.0 | 10.5x |
| Search | Easy0 | RE2 | 8 | 188.0 | 18.0 | 10.4x |
| Search | Easy0 | RE2 | 64 | 188.0 | 18.0 | 10.4x |
| Search | Easy0 | RE2 | 32K | 188.0 | 18.0 | 10.4x |
| Search | Easy1 | RE2 | 8 | 188.0 | 18.0 | 10.4x |
| Practical | EmptyPartialMatch | RE2 | 0 | 186.0 | 19.0 | 9.8x |
| FullMatch | DotStar | RE2 | 8 | 188.0 | 20.0 | 9.4x |
| Parse | DigitDs | BitState | 12 | 1,342 | 146.0 | 9.2x |
| FullMatch | DotStar | RE2 | 64 | 183.0 | 20.0 | 9.2x |
| FullMatch | DotStar | RE2 | 512 | 183.0 | 20.0 | 9.2x |
| Parse | Digits | BitState | 12 | 1,338 | 147.0 | 9.1x |
| FullMatch | DotStar | RE2 | 4K | 182.0 | 20.0 | 9.1x |
| FullMatch | DotStar | RE2 | 32K | 182.0 | 20.0 | 9.1x |
| FullMatch | DotStar | RE2 | 256K | 182.0 | 20.0 | 9.1x |
| FullMatch | DotStar | RE2 | 2M | 181.0 | 20.0 | 9.1x |
| Practical | SimplePartialMatch | RE2 | 11 | 253.0 | 29.0 | 8.7x |
| Parse | Split | BitState | 12 | 1,259 | 148.0 | 8.5x |
| Search | Medium | DFA | 8 | 141.0 | 17.0 | 8.3x |
| Search | Hard | DFA | 8 | 139.0 | 17.0 | 8.2x |
| Search | Parens | DFA | 8 | 139.0 | 17.0 | 8.2x |
| Search | Easy1 | NFA | 64 | 1,725 | 219.0 | 7.9x |
| Search | Easy0 | DFA | 8 | 106.0 | 14.0 | 7.6x |
| FullMatch | DotStarDollar | RE2 | 8 | 308.0 | 41.0 | 7.5x |
| Search | Easy1 | DFA | 8 | 101.0 | 14.0 | 7.2x |
| FullMatch | DotStarCapture | RE2 | 8 | 307.0 | 44.0 | 7.0x |
| Search | Easy0 | DFA | 64 | 122.0 | 18.0 | 6.8x |
| Search | Easy0 | NFA | 8 | 1,151 | 177.0 | 6.5x |
| Search | Easy1 | NFA | 8 | 1,128 | 174.0 | 6.5x |
| Practical | SmallHTTPPartialMatch | RE2 | 17 | 303.0 | 47.0 | 6.4x |
| Search | Easy0 | NFA | 64 | 1,168 | 184.0 | 6.3x |
| Parse | DigitDs | RE2 | 12 | 339.0 | 55.0 | 6.2x |
| Parse | Digits | RE2 | 12 | 339.0 | 55.0 | 6.2x |
| Search | Easy0 | NFA | 512 | 1,208 | 205.0 | 5.9x |
| Parse | Split | RE2 | 12 | 252.0 | 43.0 | 5.9x |
| Search | Easy1 | DFA | 64 | 122.0 | 21.0 | 5.8x |
| Compile | Regexp_Simplify | Simplify | 0 | 4,828 | 985.0 | 4.9x |
| Search | Hard | DFA | 64 | 517.0 | 107.0 | 4.8x |
| Search | Medium | DFA | 64 | 509.0 | 108.0 | 4.7x |
| Search | Parens | DFA | 64 | 500.0 | 109.0 | 4.6x |
| FullMatch | DotStarDollar | RE2 | 64 | 668.0 | 148.0 | 4.5x |
| FullMatch | DotStarCapture | RE2 | 64 | 669.0 | 149.0 | 4.5x |
| Search | Easy0 | DFA | 512 | 165.0 | 38.0 | 4.3x |
| Search | Easy1 | DFA | 512 | 199.0 | 49.0 | 4.1x |
| Compile | Regexp_Parse | Parse | 0 | 3,002 | 751.0 | 4.0x |
| FullMatch | DotStarCapture | RE2 | 512 | 3,625 | 990.0 | 3.7x |
| Search | Hard | DFA | 512 | 3,515 | 962.0 | 3.7x |
| FullMatch | DotStarDollar | RE2 | 512 | 3,594 | 985.0 | 3.6x |
| Search | Medium | DFA | 512 | 3,477 | 959.0 | 3.6x |
| Search | Parens | DFA | 512 | 3,397 | 945.0 | 3.6x |
| FullMatch | DotStarDollar | RE2 | 4K | 27,104 | 7,667 | 3.5x |
| Search | Easy0 | NFA | 4K | 1,481 | 419.0 | 3.5x |
| Search | Medium | DFA | 4K | 27,156 | 7,699 | 3.5x |
| FullMatch | DotStarCapture | RE2 | 2M | 13,786,400 | 3,974,130 | 3.5x |
| Parse | Split | OnePass | 12 | 104.0 | 30.0 | 3.5x |
| Search | Hard | DFA | 16M | 109,172,000 | 31,519,600 | 3.5x |
| Search | Parens | DFA | 256K | 1,695,590 | 489,612 | 3.5x |
| Search | Parens | DFA | 4K | 26,523 | 7,665 | 3.5x |
| Search | Medium | DFA | 256K | 1,714,450 | 496,137 | 3.5x |
| Search | Hard | DFA | 2M | 13,607,400 | 3,938,500 | 3.5x |
| Search | Medium | DFA | 2M | 13,674,200 | 3,958,440 | 3.5x |
| FullMatch | DotStarCapture | RE2 | 256K | 1,711,880 | 495,915 | 3.5x |
| FullMatch | DotStarCapture | RE2 | 4K | 26,744 | 7,758 | 3.4x |
| Parse | DigitDs | OnePass | 12 | 148.0 | 43.0 | 3.4x |
| Search | Parens | DFA | 16M | 108,267,000 | 31,465,100 | 3.4x |
| Search | Medium | DFA | 32K | 214,919 | 62,474 | 3.4x |
| FullMatch | DotStarDollar | RE2 | 32K | 210,967 | 61,330 | 3.4x |
| FullMatch | DotStarCapture | RE2 | 32K | 210,810 | 61,375 | 3.4x |
| Search | Hard | DFA | 32K | 213,540 | 62,218 | 3.4x |
| Search | Parens | DFA | 32K | 211,490 | 61,648 | 3.4x |
| Search | Hard | DFA | 4K | 26,630 | 7,766 | 3.4x |
| Search | Parens | DFA | 2M | 13,572,900 | 3,960,060 | 3.4x |
| Search | Medium | DFA | 16M | 108,600,000 | 31,691,300 | 3.4x |
| Parse | Digits | OnePass | 12 | 147.0 | 43.0 | 3.4x |
| Search | Hard | DFA | 256K | 1,697,190 | 497,619 | 3.4x |
| FullMatch | DotStarDollar | RE2 | 2M | 13,628,000 | 4,003,980 | 3.4x |
| FullMatch | DotStarDollar | RE2 | 256K | 1,705,500 | 507,182 | 3.4x |
| Search | Easy1 | DFA | 256K | 56,966 | 18,351 | 3.1x |
| Search | Easy1 | DFA | 32K | 7,080 | 2,389 | 3.0x |
| Search | Easy1 | DFA | 4K | 721.0 | 260.0 | 2.8x |
| Search | Easy0 | NFA | 256K | 39,386 | 16,912 | 2.3x |
| Practical | HTTPPartialMatch | RE2 | 93 | 760.0 | 331.0 | 2.3x |
| Search | Easy1 | DFA | 2M | 453,287 | 217,101 | 2.1x |
| Search | Easy0 | NFA | 32K | 4,747 | 2,360 | 2.0x |
| Search | Easy0 | DFA | 4K | 437.0 | 239.0 | 1.8x |
| Search | Easy1 | DFA | 16M | 3,615,010 | 2,022,750 | 1.8x |
| Search | Easy0 | DFA | 256K | 26,966 | 15,907 | 1.7x |
| Search | Easy0 | DFA | 2M | 293,114 | 176,725 | 1.7x |
| Search | Easy0 | DFA | 32K | 3,165 | 2,080 | 1.5x |
| Search | Easy0 | DFA | 16M | 2,480,910 | 1,817,500 | 1.4x |

## Summary Statistics

- Total benchmarks compared: 154
- Mean speedup (O0 -> O3): 8.8x
- Median speedup: 9.1x
- Max speedup: 28.9x
- Min speedup: 1.4x
- Benchmarks >2x faster with O3: 148
- Benchmarks >5x faster with O3: 100
