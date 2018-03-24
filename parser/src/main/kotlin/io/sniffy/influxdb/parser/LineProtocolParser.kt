package io.sniffy.influxdb.parser

import io.sniffy.influxdb.lineprotocol.Point
import java.io.*
import java.lang.Long.parseLong

class LineProtocolParser(private val reader: Reader, private val failFast: Boolean = false) :
        Iterator<Point>, Closeable {

    private val sb = StringBuilder()
    private val sb2 = StringBuilder()

    private var nextPoint: Point? = null

    constructor(string: String, failFast: Boolean = false) :
            this(StringReader(string), failFast)

    constructor(inputStream: InputStream, failFast: Boolean = false) :
            this(InputStreamReader(inputStream, Charsets.UTF_8), failFast)

    @Synchronized
    override fun hasNext(): Boolean {
        nextPoint = parseNext()
        return nextPoint != null
    }

    @Synchronized
    override fun next(): Point {
        return nextPoint ?: throw NoSuchElementException()
    }

    override fun close() {
        reader.close()
    }

    private fun parseNext(): Point? {

        val builder = Point.Builder()

        var state = State.Beginning

        sb.setLength(0)
        sb2.setLength(0)

        while (!state.finite()) state = state.parse(reader, sb, sb2, builder)

        return builder.build()

    }

    private enum class State {

        // TODO: what about \r ??

        Beginning {

            override fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Eos
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '#' -> Comment
                        '\n' -> Beginning
                        else -> {
                            sb.append(c1)
                            Measurement
                        }
                    }
                }
            }

        },

        Comment {

            override fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
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

            override fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {

                val i1 = reader.read()
                if (i1 < 0) return Error
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> {
                            val i2 = reader.read()
                            return if (i2 < 0) Error
                            else {
                                val c2 = i2.toChar()
                                when (c2) {
                                    '\n' -> Error
                                    ',', ' ' -> {
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
                            val measurement = sb.toString()
                            sb.setLength(0)
                            return if (measurement.isEmpty()) {
                                Error // TODO: read till end of line
                            } else {
                                builder.measurement = measurement
                                if (',' == c1) TagKey else FieldKey
                            }
                        }
                        else -> {
                            sb.append(c1)
                            return this
                        }
                    }
                }

            }

        },

        TagKey {

            override fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                if (i1 < 0) return Error
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> {
                            val i2 = reader.read()
                            return if (i2 < 0) Error
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
                            return Error
                            // TODO: read till new line
                        }
                        '=' -> {
                            return TagValue
                        }
                        else -> {
                            sb.append(c1)
                            return this
                        }
                    }
                }
            }

        },
        TagValue {

            override fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                if (i1 < 0) return Error
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> {
                            val i2 = reader.read()
                            return if (i2 < 0) Error
                            else {
                                val c2 = i2.toChar()
                                when (c2) {
                                    '\n' -> Error
                                    ',', ' ', '=' -> {
                                        sb2.append(c2)
                                        this
                                    }
                                    else -> {
                                        sb2.append('\\').append(c2)
                                        this
                                    }
                                }
                            }
                        }
                        ',', ' ' -> {
                            val tagKey = sb.toString()
                            val tagValue = sb2.toString()
                            sb.setLength(0)
                            sb2.setLength(0)
                            if (tagKey.isEmpty() || tagValue.isEmpty()) {
                                return Error // TODO: skip if fail fast or read till new Line
                            } else {
                                builder.addTag(tagKey, tagValue)
                                return if (',' == c1) TagKey else FieldKey
                            }
                        }
                        '\n' -> return Error
                        '=' -> {
                            return Error // TODO: read till new Line
                        }
                        else -> {
                            sb2.append(c1)
                            return this
                        }
                    }
                }
            }
        },

        FieldKey {

            override fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                if (i1 < 0) return Error
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> {
                            val i2 = reader.read()
                            return if (i2 < 0) Error
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
                            return Error
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

            override fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                return if (i1 < 0) Error
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

            override fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                if (i1 < 0) return Error
                else {
                    val c1 = i1.toChar()
                    when (c1) {
                        '\\' -> {
                            val i2 = reader.read()
                            return if (i2 < 0) Error
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
                            return if (i2 < 0) End
                            else when (i2.toChar()) {
                                '\n' -> End
                                ',' -> FieldKey
                                ' ' -> Timestamp
                                else -> Error
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

            override fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
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

                    return End
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

            override fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State {
                val i1 = reader.read()
                if (i1 < 0) {
                    builder.timestamp = parseLong(sb.toString())
                    return End
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

        open fun parse(reader: Reader, sb: StringBuilder, sb2: StringBuilder, builder: Point.Builder): State = this

    }

}