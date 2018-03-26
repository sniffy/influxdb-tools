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

    constructor(string: String, failFast: Boolean = false) :
            this(StringReader(string), failFast)

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

        return builder.build()

    }

    private enum class State {

        // TODO: support fail-fast
        // TODO: what about carriage return
        // TODO: what about multiple spaces

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
                            this
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
                            this
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
                            this
                        }
                    }
                }
            }

        },
        TagKeyEscape {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                if (i1 < 0) return Eos
                else {
                    val c1 = i1.toChar()
                    return when (c1) {
                        '\n' -> Error
                        '\\' -> {
                            sb.append('\\')
                            this
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
                if (i1 < 0) return Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> return TagValueEscape
                        ',', ' ' -> {
                            val tagKey = sb.toString()
                            val tagValue = sb2.toString()
                            sb.setLength(0)
                            sb2.setLength(0)
                            return if (tagKey.isEmpty() || tagValue.isEmpty()) {
                                ErrorInLine // TODO: skip if fail fast
                            } else {
                                builder.addTag(tagKey, tagValue)
                                if (',' == c1) TagKey else FieldKey
                            }
                        }
                        '\n' -> return Error
                        '=' -> {
                            return ErrorInLine // TODO: read till new Line
                        }
                        else -> {
                            sb2.append(c1)
                            return this
                        }
                    }
                }
            }
        },
        TagValueEscape {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                if (i1 < 0) return Eos
                else {
                    val c1 = i1.toChar()
                    return when (c1) {
                        '\n' -> Error
                        '\\' -> {
                            sb2.append('\\')
                            this
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

        FieldKey {

            override fun parse(reader: PushbackReader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                if (i1 < 0) return Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> {
                            val i2 = reader.read()
                            return if (i2 < 0) Eos
                            else {
                                val c2 = i2.toChar()
                                when (c2) {
                                    '\n' -> Error
                                    ',', ' ', '=' -> {
                                        sb.append(c2)
                                        this
                                    }
                                    else -> {
                                        sb.append('\\').append(c2)
                                        this
                                    }
                                }
                            }
                        }
                        '\n' -> return Error
                        ',', ' ' -> {
                            return ErrorInLine
                        }
                        '=' -> {
                            return FieldValue
                        }
                        else -> {
                            sb.append(c1)
                            return this
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
                if (i1 < 0) return Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> {
                            val i2 = reader.read()
                            return if (i2 < 0) Eos
                            else {
                                val c2 = i2.toChar()
                                when (c2) {
                                    '"' -> sb2.append('"')
                                    else -> sb2.append('\\').append(c2)
                                }
                                this
                            }
                        }
                        '"' -> {
                            builder.addValue(sb.toString(), sb2.toString())
                            sb.setLength(0)
                            sb2.setLength(0)
                            val i2 = reader.read()
                            return if (i2 < 0) Eos
                            else when (i2.toChar()) {
                                '\n' -> End
                                ',' -> FieldKey
                                ' ' -> Timestamp
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
                            return if (',' == c1) FieldKey else Timestamp
                        }
                        else -> {
                            sb2.append(c1)
                            return this
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
                return if (i1 < 0) Eos
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