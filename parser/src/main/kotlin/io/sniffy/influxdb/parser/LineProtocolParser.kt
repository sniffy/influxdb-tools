package io.sniffy.influxdb.parser

import io.sniffy.influxdb.lineprotocol.Point
import java.io.*
import java.lang.Long.parseLong

class LineProtocolParser(reader: Reader, private val failFast: Boolean = false) :
        Iterator<Point>, Closeable {

    private val reader: PushbackReader

    init {
        this.reader = PushbackReader(reader, 1)
    }

    private val sb = StringBuilder()
    private val sb2 = StringBuilder()

    private var nextPoint: Point? = null
    private var state = State.Beginning

    @JvmOverloads
    constructor(string: String, failFast: Boolean = false) :
            this(StringReader(string), failFast)

    @JvmOverloads
    constructor(inputStream: InputStream, failFast: Boolean = false) :
            this(InputStreamReader(inputStream, Charsets.UTF_8), failFast)

    @Synchronized
    override fun hasNext(): Boolean {
        return if (state == State.Eos) {
            false
        } else {
            nextPoint = parseNext()
            nextPoint != null
        }
    }

    @Synchronized
    override fun next(): Point {
        return nextPoint ?: throw NoSuchElementException()
    }

    override fun close() {
        reader.close()
    }

    private fun parseNext(): Point? {

        state = State.Beginning

        val builder = Point.Builder()

        sb.setLength(0)
        sb2.setLength(0)

        while (!state.finite()) state = state.parse(reader, sb, sb2, builder)

        return if (state == State.Error && failFast) null else builder.build()

    }

    private enum class State {

        // TODO: support fail-fast
        // TODO: what about carriage return

        Beginning {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '#' -> Comment
                        '\n' -> Beginning
                        ',', ' ' -> ErrorInLine
                        else -> {
                            reader.unread(i1)
                            Measurement
                        }
                    }
                }
            }

        },

        Comment {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\n' -> Beginning
                        else -> Comment
                    }
                }
            }

        },

        Measurement {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> MeasurementEscape
                        '\n' -> Error
                        ',', ' ' -> {
                            val measurement = sb.toString()
                            sb.setLength(0)
                            if (measurement.isEmpty()) {
                                ErrorInLine
                            } else {
                                builder.measurement = measurement
                                if (',' == c1) TagKey else FieldKey
                            }
                        }
                        else -> {
                            sb.append(c1)
                            Measurement
                        }
                    }
                }

            }

        },

        MeasurementEscape {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\n' -> ErrorInLine
                        '\\' -> {
                            sb.append('\\')
                            MeasurementEscape
                        }
                        ',', ' ' -> {
                            sb.append(c1)
                            Measurement
                        }
                        else -> {
                            sb.append('\\').append(c1)
                            Measurement
                        }
                    }
                }

            }

        },

        TagKey {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> TagKeyEscape
                        '\n' -> Error
                        ',', ' ' -> ErrorInLine
                        '=' -> TagValue
                        else -> {
                            sb.append(c1)
                            TagKey
                        }
                    }
                }
            }

        },
        TagKeyEscape {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\n' -> Error
                        '\\' -> {
                            sb.append('\\')
                            TagKeyEscape
                        }
                        ',', ' ', '=' -> {
                            sb.append(c1)
                            TagKey
                        }
                        else -> {
                            sb.append('\\').append(c1)
                            TagKey
                        }
                    }
                }
            }

        },
        TagValue {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> TagValueEscape
                        ',', ' ' -> {
                            val tagKey = sb.toString()
                            val tagValue = sb2.toString()
                            sb.setLength(0)
                            sb2.setLength(0)
                            if (tagKey.isEmpty() || tagValue.isEmpty()) {
                                ErrorInLine // TODO: skip if fail fast
                            } else {
                                builder.addTag(tagKey, tagValue)
                                if (',' == c1) TagKey else FieldKeySeparator
                            }
                        }
                        '\n' -> Error
                        '=' -> {
                            ErrorInLine
                        }
                        else -> {
                            sb2.append(c1)
                            TagValue
                        }
                    }
                }
            }
        },
        TagValueEscape {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\n' -> Error
                        '\\' -> {
                            sb2.append('\\')
                            TagValueEscape
                        }
                        ',', ' ', '=' -> {
                            sb2.append(c1)
                            TagValue
                        }
                        else -> {
                            sb2.append('\\').append(c1)
                            TagValue
                        }
                    }
                }
            }
        },

        FieldKeySeparator {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        ' ' -> FieldKeySeparator
                        else -> {
                            reader.unread(i1)
                            FieldKey
                        }
                    }
                }
            }

        },
        FieldKey {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> FieldKeyEscape
                        '\n' -> Error
                        ',', ' ' -> ErrorInLine
                        '=' -> FieldValue
                        else -> {
                            sb.append(c1)
                            FieldKey
                        }
                    }
                }
            }

        },
        FieldKeyEscape {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\n' -> Error
                        '\\' -> {
                            sb.append('\\')
                            FieldKeyEscape
                        }
                        ',', ' ', '=' -> {
                            sb.append(c1)
                            FieldKey
                        }
                        else -> {
                            sb.append('\\').append(c1)
                            FieldKey
                        }
                    }
                }
            }

        },
        FieldValue {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\n' -> Error
                        '"' -> StringFieldValue
                        else -> {
                            sb2.append(c1)
                            NonStringFieldValue
                        }
                    }
                }
            }
        },
        StringFieldValue {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> StringFieldValueEscape
                        '"' -> {
                            builder.addValue(sb.toString(), sb2.toString())
                            sb.setLength(0)
                            sb2.setLength(0)
                            val i2 = reader.read()
                            if (i2 < 0) Eos
                            else when (i2.toChar()) {
                                '\n' -> End
                                ',' -> FieldKey
                                ' ' -> TimestampSeparator
                                else -> ErrorInLine
                            }
                        }
                        else -> {
                            sb2.append(c1)
                            StringFieldValue
                        }
                    }
                }
            }

        },
        StringFieldValueEscape {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> {
                            sb2.append('\\')
                            StringFieldValue
                        }
                        '"' -> {
                            sb2.append('"')
                            StringFieldValue
                        }
                        else -> {
                            sb2.append('\\').append(c1)
                            StringFieldValue
                        }
                    }
                }
            }

        },
        NonStringFieldValue {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                if (i1 < 0) {

                    val value = sb2.toString()

                    when (value) {
                        "t", "T", "true", "True", "TRUE" -> {
                            builder.addValue(sb.toString(), true)
                        }
                        "f", "F", "false", "False", "FALSE" -> {
                            builder.addValue(sb.toString(), false)
                        }
                        else -> {
                            if (value.endsWith('i')) {
                                builder.addValue(
                                        sb.toString(),
                                        parseLong(value.substring(0, value.length - 1))
                                )
                            } else {
                                builder.addValue(
                                        sb.toString(),
                                        java.lang.Double.parseDouble(value)
                                )
                            }
                        }
                    }

                    sb.setLength(0)
                    sb2.setLength(0)

                    return Eos
                } else {
                    val c1 = i1.toChar()
                    when (c1) {
                        ',', ' ', '\n' -> {

                            val value = sb2.toString()

                            when (value) {
                                "t", "T", "true", "True", "TRUE" -> {
                                    builder.addValue(sb.toString(), true)
                                }
                                "f", "F", "false", "False", "FALSE" -> {
                                    builder.addValue(sb.toString(), false)
                                }
                                else -> {
                                    if (value.endsWith('i')) {
                                        builder.addValue(
                                                sb.toString(),
                                                parseLong(value.substring(0, value.length - 1))
                                        )
                                    } else {
                                        builder.addValue(
                                                sb.toString(),
                                                java.lang.Double.parseDouble(value)
                                        )
                                    }
                                }
                            }

                            sb.setLength(0)
                            sb2.setLength(0)
                            return when (c1) {
                                ',' -> FieldKey
                                ' ' -> TimestampSeparator
                                '\n' -> End
                                else -> ErrorInLine
                            }
                        }
                        else -> {
                            sb2.append(c1)
                            return this
                        }
                    }
                }
            }

        },

        TimestampSeparator {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        ' ' -> TimestampSeparator
                        else -> {
                            reader.unread(i1)
                            Timestamp
                        }
                    }
                }
            }

        },
        Timestamp {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                if (i1 < 0) {
                    builder.timestamp = parseLong(sb.toString())
                    return Eos
                } else {
                    val c1 = i1.toChar()
                    return when (c1) {
                        '\n' -> {
                            builder.timestamp = parseLong(sb.toString())
                            End
                        }
                        else -> {
                            sb.append(c1)
                            this
                        }
                    }
                }
            }

        },

        ErrorInLine {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Error
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\n' -> Error
                        else -> ErrorInLine
                    }
                }
            }

        },

        End {
            override fun finite() = true
        },
        Eos {
            override fun finite() = true
        },
        Error {
            override fun finite() = true
        };

        open fun finite() = false

        open fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State = this

    }

}