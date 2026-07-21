/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.slice.re2;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntPredicate;
import java.util.regex.Pattern;

import static io.airlift.slice.re2.CharClass.RUNEMAX;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.UNICODE_CASE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJvmUnicodeAuthority
{
    private static final List<Category> CATEGORIES = List.of(
            new Category("Cn", Character.UNASSIGNED),
            new Category("Lu", Character.UPPERCASE_LETTER),
            new Category("Ll", Character.LOWERCASE_LETTER),
            new Category("Lt", Character.TITLECASE_LETTER),
            new Category("Lm", Character.MODIFIER_LETTER),
            new Category("Lo", Character.OTHER_LETTER),
            new Category("Mn", Character.NON_SPACING_MARK),
            new Category("Me", Character.ENCLOSING_MARK),
            new Category("Mc", Character.COMBINING_SPACING_MARK),
            new Category("Nd", Character.DECIMAL_DIGIT_NUMBER),
            new Category("Nl", Character.LETTER_NUMBER),
            new Category("No", Character.OTHER_NUMBER),
            new Category("Zs", Character.SPACE_SEPARATOR),
            new Category("Zl", Character.LINE_SEPARATOR),
            new Category("Zp", Character.PARAGRAPH_SEPARATOR),
            new Category("Cc", Character.CONTROL),
            new Category("Cf", Character.FORMAT),
            new Category("Co", Character.PRIVATE_USE),
            new Category("Cs", Character.SURROGATE),
            new Category("Pd", Character.DASH_PUNCTUATION),
            new Category("Ps", Character.START_PUNCTUATION),
            new Category("Pe", Character.END_PUNCTUATION),
            new Category("Pc", Character.CONNECTOR_PUNCTUATION),
            new Category("Po", Character.OTHER_PUNCTUATION),
            new Category("Sm", Character.MATH_SYMBOL),
            new Category("Sc", Character.CURRENCY_SYMBOL),
            new Category("Sk", Character.MODIFIER_SYMBOL),
            new Category("So", Character.OTHER_SYMBOL),
            new Category("Pi", Character.INITIAL_QUOTE_PUNCTUATION),
            new Category("Pf", Character.FINAL_QUOTE_PUNCTUATION));

    private static final List<CategoryGroup> CATEGORY_GROUPS = List.of(
            new CategoryGroup("L", type -> type >= Character.UPPERCASE_LETTER && type <= Character.OTHER_LETTER),
            new CategoryGroup("M", type -> type >= Character.NON_SPACING_MARK && type <= Character.COMBINING_SPACING_MARK),
            new CategoryGroup("N", type -> type >= Character.DECIMAL_DIGIT_NUMBER && type <= Character.OTHER_NUMBER),
            new CategoryGroup("Z", type -> type >= Character.SPACE_SEPARATOR && type <= Character.PARAGRAPH_SEPARATOR),
            new CategoryGroup("C", type -> type == Character.CONTROL || type == Character.FORMAT ||
                    type == Character.SURROGATE || type == Character.PRIVATE_USE || type == Character.UNASSIGNED),
            new CategoryGroup("P", type -> type >= Character.DASH_PUNCTUATION && type <= Character.OTHER_PUNCTUATION ||
                    type == Character.INITIAL_QUOTE_PUNCTUATION || type == Character.FINAL_QUOTE_PUNCTUATION),
            new CategoryGroup("S", type -> type >= Character.MATH_SYMBOL && type <= Character.OTHER_SYMBOL));

    private static final List<PropertyExpectation> BINARY_PROPERTIES = List.of(
            new PropertyExpectation("IsAlphabetic", Character::isAlphabetic),
            new PropertyExpectation("IsAssigned", Character::isDefined),
            new PropertyExpectation("IsControl", codePoint -> Character.getType(codePoint) == Character.CONTROL),
            new PropertyExpectation("IsDigit", Character::isDigit),
            new PropertyExpectation("IsEmoji", Character::isEmoji),
            new PropertyExpectation("IsEmoji_Component", Character::isEmojiComponent),
            new PropertyExpectation("IsEmoji_Modifier", Character::isEmojiModifier),
            new PropertyExpectation("IsEmoji_Modifier_Base", Character::isEmojiModifierBase),
            new PropertyExpectation("IsEmoji_Presentation", Character::isEmojiPresentation),
            new PropertyExpectation("IsExtended_Pictographic", Character::isExtendedPictographic),
            new PropertyExpectation("IsHex_Digit", TestJvmUnicodeAuthority::isHexDigit),
            new PropertyExpectation("IsIdeographic", Character::isIdeographic),
            new PropertyExpectation("IsJoin_Control", codePoint -> codePoint == 0x200C || codePoint == 0x200D),
            new PropertyExpectation("IsLetter", Character::isLetter),
            new PropertyExpectation("IsLowercase", Character::isLowerCase),
            new PropertyExpectation("IsNoncharacter_Code_Point", TestJvmUnicodeAuthority::isNoncharacterCodePoint),
            new PropertyExpectation("IsPunctuation", TestJvmUnicodeAuthority::isPunctuation),
            new PropertyExpectation("IsTitlecase", Character::isTitleCase),
            new PropertyExpectation("IsUppercase", Character::isUpperCase),
            new PropertyExpectation("IsWhite_Space", TestJvmUnicodeAuthority::isWhiteSpace),
            new PropertyExpectation("IsWord", TestJvmUnicodeAuthority::isWord),
            new PropertyExpectation("IsAlnum", codePoint -> Character.isAlphabetic(codePoint) || Character.isDigit(codePoint)),
            new PropertyExpectation("IsBlank", codePoint -> Character.getType(codePoint) == Character.SPACE_SEPARATOR || codePoint == '\t'),
            new PropertyExpectation("IsGraph", TestJvmUnicodeAuthority::isGraph),
            new PropertyExpectation("IsPrint", codePoint -> isGraph(codePoint) ||
                    (Character.getType(codePoint) == Character.SPACE_SEPARATOR || codePoint == '\t') &&
                            Character.getType(codePoint) != Character.CONTROL));

    private static final List<PropertyExpectation> JAVA_PROPERTIES = List.of(
            new PropertyExpectation("javaLowerCase", Character::isLowerCase),
            new PropertyExpectation("javaUpperCase", Character::isUpperCase),
            new PropertyExpectation("javaAlphabetic", Character::isAlphabetic),
            new PropertyExpectation("javaIdeographic", Character::isIdeographic),
            new PropertyExpectation("javaTitleCase", Character::isTitleCase),
            new PropertyExpectation("javaDigit", Character::isDigit),
            new PropertyExpectation("javaDefined", Character::isDefined),
            new PropertyExpectation("javaLetter", Character::isLetter),
            new PropertyExpectation("javaLetterOrDigit", Character::isLetterOrDigit),
            new PropertyExpectation("javaJavaIdentifierStart", Character::isJavaIdentifierStart),
            new PropertyExpectation("javaJavaIdentifierPart", Character::isJavaIdentifierPart),
            new PropertyExpectation("javaUnicodeIdentifierStart", Character::isUnicodeIdentifierStart),
            new PropertyExpectation("javaUnicodeIdentifierPart", Character::isUnicodeIdentifierPart),
            new PropertyExpectation("javaIdentifierIgnorable", Character::isIdentifierIgnorable),
            new PropertyExpectation("javaSpaceChar", Character::isSpaceChar),
            new PropertyExpectation("javaWhitespace", Character::isWhitespace),
            new PropertyExpectation("javaISOControl", Character::isISOControl),
            new PropertyExpectation("javaMirrored", Character::isMirrored));

    @Test
    public void testGeneralCategoriesComeFromCharacterType()
    {
        for (Category category : CATEGORIES) {
            CharClass characterClass = UnicodeGroups.lookup(category.name());
            assertThat(characterClass).as(category.name()).isNotNull();
            assertRanges(characterClass, codePoint -> Character.getType(codePoint) == category.type(), category.name());
        }

        for (CategoryGroup categoryGroup : CATEGORY_GROUPS) {
            CharClass characterClass = UnicodeGroups.lookup(categoryGroup.name());
            assertThat(characterClass).as(categoryGroup.name()).isNotNull();
            assertRanges(characterClass, codePoint -> categoryGroup.types().test(Character.getType(codePoint)), categoryGroup.name());
        }
    }

    @Test
    public void testScriptsComeFromCharacterUnicodeScript()
    {
        for (Character.UnicodeScript script : Character.UnicodeScript.values()) {
            CharClass characterClass = UnicodeGroups.lookup(script.name());
            assertThat(characterClass).as(script.name()).isNotNull();
            assertRanges(characterClass, codePoint -> Character.UnicodeScript.of(codePoint) == script, script.name());
        }
    }

    @Test
    public void testJavaPropertyAliases()
    {
        assertThat(UnicodeGroups.lookup("InBasic_Latin"))
                .isSameAs(UnicodeGroups.lookup("block=Basic_Latin"));
        assertThat(UnicodeGroups.lookup("Greek"))
                .isSameAs(UnicodeGroups.lookup("script=Greek"))
                .isSameAs(UnicodeGroups.lookup("IsGreek"));
        assertThat(UnicodeGroups.lookup("Lu"))
                .isSameAs(UnicodeGroups.lookup("Uppercase_Letter"))
                .isSameAs(UnicodeGroups.lookup("general_category=Lu"));
        assertThat(UnicodeGroups.lookup("IsAlphabetic"))
                .isSameAs(UnicodeGroups.lookup("IsAlpha"));
        assertThat(UnicodeGroups.lookup("IsControl"))
                .isSameAs(UnicodeGroups.lookup("IsCntrl"));
        assertThat(UnicodeGroups.lookup("IsHex_Digit"))
                .isSameAs(UnicodeGroups.lookup("IsXDigit"));
        assertThat(UnicodeGroups.lookup("IsLowercase"))
                .isSameAs(UnicodeGroups.lookup("IsLower"));
        assertThat(UnicodeGroups.lookup("IsPunctuation"))
                .isSameAs(UnicodeGroups.lookup("IsPunct"));
        assertThat(UnicodeGroups.lookup("IsUppercase"))
                .isSameAs(UnicodeGroups.lookup("IsUpper"));
        assertThat(UnicodeGroups.lookup("IsWhite_Space"))
                .isSameAs(UnicodeGroups.lookup("IsSpace"));
        assertThat(UnicodeGroups.lookup("Not_A_Unicode_Property")).isNull();
    }

    @Test
    public void testBlocksComeFromCharacterUnicodeBlock()
    {
        List<Character.UnicodeBlock> blocks = Arrays.stream(Character.UnicodeBlock.class.getFields())
                .filter(field -> Modifier.isStatic(field.getModifiers()))
                .filter(field -> field.getType() == Character.UnicodeBlock.class)
                .map(field -> {
                    try {
                        return (Character.UnicodeBlock) field.get(null);
                    }
                    catch (IllegalAccessException e) {
                        throw new AssertionError(e);
                    }
                })
                .distinct()
                .sorted(Comparator.comparing(Object::toString))
                .toList();

        for (Character.UnicodeBlock block : blocks) {
            String propertyName = "In" + block;
            assertRanges(
                    UnicodeGroups.lookup(propertyName),
                    codePoint -> Character.UnicodeBlock.of(codePoint) == block,
                    propertyName);
        }
    }

    @Test
    public void testBinaryPropertiesComeFromJvmPredicates()
    {
        for (PropertyExpectation property : BINARY_PROPERTIES) {
            assertRanges(UnicodeGroups.lookup(property.name()), property.predicate(), property.name());
        }
    }

    @Test
    public void testJavaPropertiesComeFromCharacterPredicates()
    {
        for (PropertyExpectation property : JAVA_PROPERTIES) {
            assertRanges(UnicodeGroups.lookup(property.name()), property.predicate(), property.name());
        }
    }

    @Test
    public void testCaseFoldingUsesJvmCanonicalEquivalence()
    {
        int[] expectedGroupSize = new int[RUNEMAX + 1];
        for (int codePoint = 0; codePoint <= RUNEMAX; codePoint++) {
            expectedGroupSize[canonicalCaseFold(codePoint)]++;
        }

        for (int codePoint = 0; codePoint <= RUNEMAX; codePoint++) {
            int canonicalCodePoint = canonicalCaseFold(codePoint);
            int foldedCodePoint = codePoint;
            int actualGroupSize = 0;
            do {
                if (canonicalCaseFold(foldedCodePoint) != canonicalCodePoint) {
                    throw new AssertionError("wrong canonical fold in cycle for U+%04X".formatted(codePoint));
                }
                actualGroupSize++;
                if (actualGroupSize > expectedGroupSize[canonicalCodePoint]) {
                    throw new AssertionError("fold cycle does not close for U+%04X".formatted(codePoint));
                }
                foldedCodePoint = UnicodeCaseFold.cycleFoldRune(foldedCodePoint);
            }
            while (foldedCodePoint != codePoint);

            if (actualGroupSize != expectedGroupSize[canonicalCodePoint]) {
                throw new AssertionError("incomplete fold equivalence class for U+%04X: expected %s, got %s"
                        .formatted(codePoint, expectedGroupSize[canonicalCodePoint], actualGroupSize));
            }
        }
    }

    @Test
    public void testFocusedCaseFoldingMatchesJavaPattern()
    {
        assertCaseEquivalent('I', 'i', 0x0130, 0x0131);
        assertCaseEquivalent('S', 's', 0x017F);
        assertCaseEquivalent('K', 'k', 0x212A);
        assertCaseEquivalent(0x03A3, 0x03C3, 0x03C2);
    }

    private static void assertCaseEquivalent(int... codePoints)
    {
        for (int patternCodePoint : codePoints) {
            Pattern pattern = Pattern.compile(new String(Character.toChars(patternCodePoint)), CASE_INSENSITIVE | UNICODE_CASE);
            for (int inputCodePoint : codePoints) {
                boolean javaMatches = pattern.matcher(new String(Character.toChars(inputCodePoint))).matches();
                assertThat(inFoldCycle(patternCodePoint, inputCodePoint))
                        .as("U+%04X against U+%04X", patternCodePoint, inputCodePoint)
                        .isEqualTo(javaMatches);
            }
        }
    }

    private static boolean inFoldCycle(int initialCodePoint, int candidateCodePoint)
    {
        int codePoint = initialCodePoint;
        do {
            if (codePoint == candidateCodePoint) {
                return true;
            }
            codePoint = UnicodeCaseFold.cycleFoldRune(codePoint);
        }
        while (codePoint != initialCodePoint);
        return false;
    }

    private static int canonicalCaseFold(int codePoint)
    {
        return Character.toLowerCase(Character.toUpperCase(codePoint));
    }

    private static boolean isHexDigit(int codePoint)
    {
        return Character.isDigit(codePoint) ||
                codePoint >= 'A' && codePoint <= 'F' ||
                codePoint >= 'a' && codePoint <= 'f' ||
                codePoint >= 0xFF21 && codePoint <= 0xFF26 ||
                codePoint >= 0xFF41 && codePoint <= 0xFF46;
    }

    private static boolean isNoncharacterCodePoint(int codePoint)
    {
        return (codePoint & 0xFFFE) == 0xFFFE || codePoint >= 0xFDD0 && codePoint <= 0xFDEF;
    }

    private static boolean isPunctuation(int codePoint)
    {
        int type = Character.getType(codePoint);
        return type >= Character.DASH_PUNCTUATION && type <= Character.OTHER_PUNCTUATION ||
                type == Character.INITIAL_QUOTE_PUNCTUATION || type == Character.FINAL_QUOTE_PUNCTUATION;
    }

    private static boolean isWhiteSpace(int codePoint)
    {
        int type = Character.getType(codePoint);
        return type >= Character.SPACE_SEPARATOR && type <= Character.PARAGRAPH_SEPARATOR ||
                codePoint >= '\t' && codePoint <= '\r' || codePoint == 0x85;
    }

    private static boolean isWord(int codePoint)
    {
        int type = Character.getType(codePoint);
        return Character.isAlphabetic(codePoint) ||
                type == Character.NON_SPACING_MARK ||
                type == Character.ENCLOSING_MARK ||
                type == Character.COMBINING_SPACING_MARK ||
                type == Character.DECIMAL_DIGIT_NUMBER ||
                type == Character.CONNECTOR_PUNCTUATION ||
                codePoint == 0x200C || codePoint == 0x200D;
    }

    private static boolean isGraph(int codePoint)
    {
        int type = Character.getType(codePoint);
        return type != Character.SPACE_SEPARATOR &&
                type != Character.LINE_SEPARATOR &&
                type != Character.PARAGRAPH_SEPARATOR &&
                type != Character.CONTROL &&
                type != Character.SURROGATE &&
                type != Character.UNASSIGNED;
    }

    private static void assertRanges(CharClass characterClass, IntPredicate expected, String name)
    {
        int previousHigh = -1;
        for (RuneRange range : characterClass.ranges()) {
            if (range.low() <= previousHigh) {
                throw new AssertionError("unordered range for %s at U+%04X".formatted(name, range.low()));
            }
            for (int codePoint = previousHigh + 1; codePoint < range.low(); codePoint++) {
                if (expected.test(codePoint)) {
                    throw new AssertionError("missing U+%04X from %s".formatted(codePoint, name));
                }
            }
            for (int codePoint = range.low(); codePoint <= range.high(); codePoint++) {
                if (!expected.test(codePoint)) {
                    throw new AssertionError("unexpected U+%04X in %s".formatted(codePoint, name));
                }
            }
            previousHigh = range.high();
        }
        for (int codePoint = previousHigh + 1; codePoint <= RUNEMAX; codePoint++) {
            if (expected.test(codePoint)) {
                throw new AssertionError("missing U+%04X from %s".formatted(codePoint, name));
            }
        }
    }

    private record Category(String name, int type) {}

    private record CategoryGroup(String name, IntPredicate types) {}

    private record PropertyExpectation(String name, IntPredicate predicate) {}
}
