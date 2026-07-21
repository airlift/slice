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

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntPredicate;

import static io.airlift.slice.re2.CharClass.RUNEMAX;
import static java.util.Objects.requireNonNull;

/**
 * Resolves Unicode properties from the executing JVM and caches immutable range sets.
 */
final class UnicodeGroups
{
    private static final ConcurrentHashMap<String, CharClass> CACHE = new ConcurrentHashMap<>();

    private UnicodeGroups() {}

    public static CharClass lookup(String name)
    {
        requireNonNull(name, "name is null");
        Property property = resolve(name);
        if (property == null) {
            return null;
        }
        return CACHE.computeIfAbsent(property.identity(), ignored -> build(property.predicate()));
    }

    private static Property resolve(String name)
    {
        if (name.equals("Any") || name.equals("all")) {
            return new Property("all", ignored -> true);
        }

        int equalsIndex = name.indexOf('=');
        if (equalsIndex >= 0) {
            String propertyName = name.substring(0, equalsIndex).toLowerCase(Locale.ROOT);
            String value = name.substring(equalsIndex + 1);
            return switch (propertyName) {
                case "sc", "script" -> script(value);
                case "blk", "block" -> block(value);
                case "gc", "general_category" -> category(value);
                default -> null;
            };
        }

        if (name.startsWith("In")) {
            return block(name.substring(2));
        }
        if (name.startsWith("Is")) {
            String value = name.substring(2);
            Property property = binaryProperty(value);
            if (property == null) {
                property = category(value);
            }
            return property != null ? property : script(value);
        }

        Property property = category(name);
        if (property == null) {
            property = javaProperty(name);
        }
        if (property == null) {
            property = asciiPosixProperty(name);
        }
        return property != null ? property : script(name);
    }

    private static Property category(String name)
    {
        int mask = switch (name) {
            case "Cn", "Unassigned" -> mask(Character.UNASSIGNED);
            case "Lu", "Uppercase_Letter" -> mask(Character.UPPERCASE_LETTER);
            case "Ll", "Lowercase_Letter" -> mask(Character.LOWERCASE_LETTER);
            case "Lt", "Titlecase_Letter" -> mask(Character.TITLECASE_LETTER);
            case "Lm", "Modifier_Letter" -> mask(Character.MODIFIER_LETTER);
            case "Lo", "Other_Letter" -> mask(Character.OTHER_LETTER);
            case "Mn", "Nonspacing_Mark" -> mask(Character.NON_SPACING_MARK);
            case "Me", "Enclosing_Mark" -> mask(Character.ENCLOSING_MARK);
            case "Mc", "Spacing_Mark", "Combining_Spacing_Mark" -> mask(Character.COMBINING_SPACING_MARK);
            case "Nd", "Decimal_Number", "Decimal_Digit_Number" -> mask(Character.DECIMAL_DIGIT_NUMBER);
            case "Nl", "Letter_Number" -> mask(Character.LETTER_NUMBER);
            case "No", "Other_Number" -> mask(Character.OTHER_NUMBER);
            case "Zs", "Space_Separator" -> mask(Character.SPACE_SEPARATOR);
            case "Zl", "Line_Separator" -> mask(Character.LINE_SEPARATOR);
            case "Zp", "Paragraph_Separator" -> mask(Character.PARAGRAPH_SEPARATOR);
            case "Cc", "Control" -> mask(Character.CONTROL);
            case "Cf", "Format" -> mask(Character.FORMAT);
            case "Co", "Private_Use" -> mask(Character.PRIVATE_USE);
            case "Cs", "Surrogate" -> mask(Character.SURROGATE);
            case "Pd", "Dash_Punctuation" -> mask(Character.DASH_PUNCTUATION);
            case "Ps", "Open_Punctuation", "Start_Punctuation" -> mask(Character.START_PUNCTUATION);
            case "Pe", "Close_Punctuation", "End_Punctuation" -> mask(Character.END_PUNCTUATION);
            case "Pc", "Connector_Punctuation" -> mask(Character.CONNECTOR_PUNCTUATION);
            case "Po", "Other_Punctuation" -> mask(Character.OTHER_PUNCTUATION);
            case "Sm", "Math_Symbol" -> mask(Character.MATH_SYMBOL);
            case "Sc", "Currency_Symbol" -> mask(Character.CURRENCY_SYMBOL);
            case "Sk", "Modifier_Symbol" -> mask(Character.MODIFIER_SYMBOL);
            case "So", "Other_Symbol" -> mask(Character.OTHER_SYMBOL);
            case "Pi", "Initial_Punctuation", "Initial_Quote_Punctuation" -> mask(Character.INITIAL_QUOTE_PUNCTUATION);
            case "Pf", "Final_Punctuation", "Final_Quote_Punctuation" -> mask(Character.FINAL_QUOTE_PUNCTUATION);
            case "L", "Letter" -> mask(
                    Character.UPPERCASE_LETTER,
                    Character.LOWERCASE_LETTER,
                    Character.TITLECASE_LETTER,
                    Character.MODIFIER_LETTER,
                    Character.OTHER_LETTER);
            case "LC", "Cased_Letter" -> mask(
                    Character.UPPERCASE_LETTER,
                    Character.LOWERCASE_LETTER,
                    Character.TITLECASE_LETTER);
            case "M", "Mark" -> mask(
                    Character.NON_SPACING_MARK,
                    Character.ENCLOSING_MARK,
                    Character.COMBINING_SPACING_MARK);
            case "N", "Number" -> mask(
                    Character.DECIMAL_DIGIT_NUMBER,
                    Character.LETTER_NUMBER,
                    Character.OTHER_NUMBER);
            case "Z", "Separator" -> mask(
                    Character.SPACE_SEPARATOR,
                    Character.LINE_SEPARATOR,
                    Character.PARAGRAPH_SEPARATOR);
            case "C", "Other" -> mask(
                    Character.CONTROL,
                    Character.FORMAT,
                    Character.PRIVATE_USE,
                    Character.SURROGATE,
                    Character.UNASSIGNED);
            case "P", "Punctuation" -> mask(
                    Character.DASH_PUNCTUATION,
                    Character.START_PUNCTUATION,
                    Character.END_PUNCTUATION,
                    Character.CONNECTOR_PUNCTUATION,
                    Character.OTHER_PUNCTUATION,
                    Character.INITIAL_QUOTE_PUNCTUATION,
                    Character.FINAL_QUOTE_PUNCTUATION);
            case "S", "Symbol" -> mask(
                    Character.MATH_SYMBOL,
                    Character.CURRENCY_SYMBOL,
                    Character.MODIFIER_SYMBOL,
                    Character.OTHER_SYMBOL);
            case "LD" -> mask(
                    Character.UPPERCASE_LETTER,
                    Character.LOWERCASE_LETTER,
                    Character.TITLECASE_LETTER,
                    Character.MODIFIER_LETTER,
                    Character.OTHER_LETTER,
                    Character.DECIMAL_DIGIT_NUMBER);
            default -> 0;
        };
        if (mask == 0) {
            return null;
        }
        return new Property("category:" + mask, codePoint -> (mask & (1 << Character.getType(codePoint))) != 0);
    }

    private static Property script(String name)
    {
        try {
            Character.UnicodeScript script = Character.UnicodeScript.forName(name);
            return new Property("script:" + script.name(), codePoint -> Character.UnicodeScript.of(codePoint) == script);
        }
        catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private static Property block(String name)
    {
        try {
            Character.UnicodeBlock block = Character.UnicodeBlock.forName(name);
            return new Property("block:" + block, codePoint -> Character.UnicodeBlock.of(codePoint) == block);
        }
        catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private static Property binaryProperty(String name)
    {
        String normalizedName = name.replace("_", "").toUpperCase(Locale.ROOT);
        return switch (normalizedName) {
            case "ALPHABETIC", "ALPHA" -> new Property("binary:ALPHABETIC", Character::isAlphabetic);
            case "ASSIGNED" -> new Property("binary:ASSIGNED", Character::isDefined);
            case "CONTROL", "CNTRL" -> new Property(
                    "binary:CONTROL",
                    codePoint -> Character.getType(codePoint) == Character.CONTROL);
            case "DIGIT" -> new Property("binary:DIGIT", Character::isDigit);
            case "EMOJI" -> new Property("binary:EMOJI", Character::isEmoji);
            case "EMOJICOMPONENT" -> new Property("binary:EMOJICOMPONENT", Character::isEmojiComponent);
            case "EMOJIMODIFIER" -> new Property("binary:EMOJIMODIFIER", Character::isEmojiModifier);
            case "EMOJIMODIFIERBASE" -> new Property("binary:EMOJIMODIFIERBASE", Character::isEmojiModifierBase);
            case "EMOJIPRESENTATION" -> new Property("binary:EMOJIPRESENTATION", Character::isEmojiPresentation);
            case "EXTENDEDPICTOGRAPHIC" -> new Property("binary:EXTENDEDPICTOGRAPHIC", Character::isExtendedPictographic);
            case "HEXDIGIT", "XDIGIT" -> new Property("binary:HEXDIGIT", UnicodeGroups::isHexDigit);
            case "IDEOGRAPHIC" -> new Property("binary:IDEOGRAPHIC", Character::isIdeographic);
            case "JOINCONTROL" -> new Property(
                    "binary:JOINCONTROL",
                    codePoint -> codePoint == 0x200C || codePoint == 0x200D);
            case "LETTER" -> new Property("binary:LETTER", Character::isLetter);
            case "LOWERCASE", "LOWER" -> new Property("binary:LOWERCASE", Character::isLowerCase);
            case "NONCHARACTERCODEPOINT" -> new Property("binary:NONCHARACTERCODEPOINT", UnicodeGroups::isNoncharacterCodePoint);
            case "PUNCTUATION", "PUNCT" -> new Property("binary:PUNCTUATION", UnicodeGroups::isPunctuation);
            case "TITLECASE" -> new Property("binary:TITLECASE", Character::isTitleCase);
            case "UPPERCASE", "UPPER" -> new Property("binary:UPPERCASE", Character::isUpperCase);
            case "WHITESPACE", "SPACE" -> new Property("binary:WHITESPACE", UnicodeGroups::isWhiteSpace);
            case "WORD" -> new Property("binary:WORD", UnicodeGroups::isWord);
            case "ALNUM" -> new Property(
                    "binary:ALNUM",
                    codePoint -> Character.isAlphabetic(codePoint) || Character.isDigit(codePoint));
            case "BLANK" -> new Property(
                    "binary:BLANK",
                    codePoint -> Character.getType(codePoint) == Character.SPACE_SEPARATOR || codePoint == '\t');
            case "GRAPH" -> new Property("binary:GRAPH", UnicodeGroups::isGraph);
            case "PRINT" -> new Property(
                    "binary:PRINT",
                    codePoint -> isGraph(codePoint) ||
                            (Character.getType(codePoint) == Character.SPACE_SEPARATOR || codePoint == '\t') &&
                                    Character.getType(codePoint) != Character.CONTROL);
            default -> null;
        };
    }

    private static Property javaProperty(String name)
    {
        IntPredicate predicate = switch (name) {
            case "javaLowerCase" -> Character::isLowerCase;
            case "javaUpperCase" -> Character::isUpperCase;
            case "javaAlphabetic" -> Character::isAlphabetic;
            case "javaIdeographic" -> Character::isIdeographic;
            case "javaTitleCase" -> Character::isTitleCase;
            case "javaDigit" -> Character::isDigit;
            case "javaDefined" -> Character::isDefined;
            case "javaLetter" -> Character::isLetter;
            case "javaLetterOrDigit" -> Character::isLetterOrDigit;
            case "javaJavaIdentifierStart" -> Character::isJavaIdentifierStart;
            case "javaJavaIdentifierPart" -> Character::isJavaIdentifierPart;
            case "javaUnicodeIdentifierStart" -> Character::isUnicodeIdentifierStart;
            case "javaUnicodeIdentifierPart" -> Character::isUnicodeIdentifierPart;
            case "javaIdentifierIgnorable" -> Character::isIdentifierIgnorable;
            case "javaSpaceChar" -> Character::isSpaceChar;
            case "javaWhitespace" -> Character::isWhitespace;
            case "javaISOControl" -> Character::isISOControl;
            case "javaMirrored" -> Character::isMirrored;
            default -> null;
        };
        return predicate == null ? null : new Property("java:" + name, predicate);
    }

    private static Property asciiPosixProperty(String name)
    {
        IntPredicate predicate = switch (name) {
            case "ASCII" -> codePoint -> codePoint <= 0x7F;
            case "Alnum" -> codePoint -> isAsciiLetter(codePoint) || isAsciiDigit(codePoint);
            case "Alpha" -> UnicodeGroups::isAsciiLetter;
            case "Blank" -> codePoint -> codePoint == ' ' || codePoint == '\t';
            case "Cntrl" -> codePoint -> codePoint <= 0x1F || codePoint == 0x7F;
            case "Digit" -> UnicodeGroups::isAsciiDigit;
            case "Graph" -> codePoint -> codePoint >= 0x21 && codePoint <= 0x7E;
            case "Lower" -> codePoint -> codePoint >= 'a' && codePoint <= 'z';
            case "Print" -> codePoint -> codePoint >= 0x20 && codePoint <= 0x7E;
            case "Punct" -> codePoint -> codePoint >= 0x21 && codePoint <= 0x7E &&
                    !isAsciiLetter(codePoint) && !isAsciiDigit(codePoint);
            case "Space" -> codePoint -> codePoint == ' ' || codePoint >= '\t' && codePoint <= '\r';
            case "Upper" -> codePoint -> codePoint >= 'A' && codePoint <= 'Z';
            case "XDigit" -> codePoint -> isAsciiDigit(codePoint) ||
                    codePoint >= 'A' && codePoint <= 'F' || codePoint >= 'a' && codePoint <= 'f';
            default -> null;
        };
        return predicate == null ? null : new Property("posix:" + name, predicate);
    }

    private static CharClass build(IntPredicate predicate)
    {
        CharClassBuilder characterClassBuilder = new CharClassBuilder();
        int rangeStart = -1;
        for (int codePoint = 0; codePoint <= RUNEMAX; codePoint++) {
            if (predicate.test(codePoint)) {
                if (rangeStart < 0) {
                    rangeStart = codePoint;
                }
            }
            else if (rangeStart >= 0) {
                characterClassBuilder.addRange(rangeStart, codePoint - 1);
                rangeStart = -1;
            }
        }
        if (rangeStart >= 0) {
            characterClassBuilder.addRange(rangeStart, RUNEMAX);
        }
        return characterClassBuilder.toCharClass();
    }

    private static int mask(int... types)
    {
        int mask = 0;
        for (int type : types) {
            mask |= 1 << type;
        }
        return mask;
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

    private static boolean isAsciiLetter(int codePoint)
    {
        return codePoint >= 'A' && codePoint <= 'Z' || codePoint >= 'a' && codePoint <= 'z';
    }

    private static boolean isAsciiDigit(int codePoint)
    {
        return codePoint >= '0' && codePoint <= '9';
    }

    private record Property(String identity, IntPredicate predicate) {}
}
