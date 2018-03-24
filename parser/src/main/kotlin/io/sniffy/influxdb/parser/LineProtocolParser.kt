package io.sniffy.influxdb.parser

import io.sniffy.influxdb.lineprotocol.Point
import io.sniffy.influxdb.parser.LineProtocolParser.State.*
import java.io.InputStream
import java.io.InputStreamReader
import java.io.Reader
import java.io.StringReader
import java.lang.Long.parseLong

class LineProtocolParser(private val reader: Reader, private val failFast: Boolean = false) : Iterator<Point> {

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

    private enum class State {
        Beginning,

        Comment,

        Measurement,

        TagKey,
        TagValue,

        FieldKey,
        FieldValue,
        StringFieldValue,
        NonStringFieldValue,

        Timestamp,

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

    }

    private fun parseNext(): Point? {

        val builder = Point.Builder()

        var state = State.Beginning

        sb.setLength(0)
        sb2.setLength(0)

        // TODO: what about \r ??

        while (!state.finite()) {
            when (state) {
                Beginning -> {
                    val i1 = reader.read()
                    if (i1 < 0) state = Eos
                    else {
                        val c1 = i1.toChar()
                        when (c1) {
                            '#' -> state = Comment
                            else -> {
                                sb.append(c1)
                                state = Measurement
                            }
                        }
                    }
                }
                Comment -> {
                    val i1 = reader.read()
                    if (i1 < 0) state = Eos
                    else {
                        val c1 = i1.toChar()
                        when (c1) {
                            '\n' -> state = Beginning
                        }
                    }

                }
                Measurement -> {
                    val i1 = reader.read()
                    if (i1 < 0) state = Error
                    else {
                        val c1 = i1.toChar()
                        when (c1) {
                            '\\' -> {
                                val i2 = reader.read()
                                if (i2 < 0) state = Error
                                else {
                                    val c2 = i2.toChar()
                                    when (c2) {
                                        '\n' -> state = Error
                                        ',' -> sb.append(',')
                                        ' ' -> sb.append(' ')
                                        else -> sb.append('\\').append(c2)
                                    }
                                }
                            }
                            '\n' -> state = Error
                            ',', ' ' -> {
                                val measurement = sb.toString()
                                if (measurement.isEmpty()) {
                                    state = Error // TODO: read till end of line
                                } else {
                                    builder.measurement = measurement
                                    state = if (',' == c1) TagKey else FieldKey
                                }
                                sb.setLength(0)
                            }
                            else -> sb.append(c1)
                        }
                    }
                }
                TagKey -> {
                    val i1 = reader.read()
                    if (i1 < 0) state = Error
                    else {
                        val c1 = i1.toChar()
                        when (c1) {
                            '\\' -> {
                                val i2 = reader.read()
                                if (i2 < 0) state = Error
                                else {
                                    val c2 = i2.toChar()
                                    when (c2) {
                                        '\n' -> state = Error
                                        ',' -> sb.append(',')
                                        ' ' -> sb.append(' ')
                                        '=' -> sb.append('=')
                                        else -> sb.append('\\').append(c2)
                                    }
                                }
                            }
                            '\n' -> state = Error
                            ',', ' ' -> {
                                state = Error
                                // TODO: read till new line
                            }
                            '=' -> {
                                state = TagValue
                            }
                            else -> sb.append(c1)
                        }
                    }
                }
                TagValue -> {
                    val i1 = reader.read()
                    if (i1 < 0) state = Error
                    else {
                        val c1 = i1.toChar()
                        when (c1) {
                            '\\' -> {
                                val i2 = reader.read()
                                if (i2 < 0) state = Error
                                else {
                                    val c2 = i2.toChar()
                                    when (c2) {
                                        '\n' -> state = Error
                                        ',' -> sb2.append(',')
                                        ' ' -> sb2.append(' ')
                                        '=' -> sb2.append('=')
                                        else -> sb2.append('\\').append(c2)
                                    }
                                }
                            }
                            ',', ' ' -> {
                                val tagKey = sb.toString()
                                val tagValue = sb2.toString()
                                if (tagKey.isEmpty() || tagValue.isEmpty()) {
                                    state = Error // TODO: skip if fail fast or read till new Line
                                } else {
                                    builder.addTag(tagKey, tagValue)
                                    state = if (',' == c1) TagKey else FieldKey
                                }
                                sb.setLength(0)
                                sb2.setLength(0)
                            }
                            '\n' -> state = Error
                            '=' -> {
                                state = Error // TODO: read till new Line
                            }
                            else -> sb2.append(c1)
                        }
                    }
                }

                FieldKey -> {
                    val i1 = reader.read()
                    if (i1 < 0) state = Error
                    else {
                        val c1 = i1.toChar()
                        when (c1) {
                            '\\' -> {
                                val i2 = reader.read()
                                if (i2 < 0) state = Error
                                else {
                                    val c2 = i2.toChar()
                                    when (c2) {
                                        '\n' -> state = Error
                                        ',' -> sb.append(',')
                                        ' ' -> sb.append(' ')
                                        '=' -> sb.append('=')
                                        else -> sb.append('\\').append(c2)
                                    }
                                }
                            }
                            '\n' -> state = Error
                            ',', ' ' -> {
                                state = Error
                            }
                            '=' -> {
                                state = FieldValue
                            }
                            else -> sb.append(c1)
                        }
                    }
                }
                FieldValue -> {
                    val i1 = reader.read()
                    if (i1 < 0) state = Error
                    else {
                        val c1 = i1.toChar()
                        when (c1) {
                            '\n' -> state = Error
                            '"' -> state = StringFieldValue
                            else -> {
                                sb2.append(c1)
                                state = NonStringFieldValue
                            }
                        }
                    }
                }
                StringFieldValue -> {
                    val i1 = reader.read()
                    if (i1 < 0) state = Error
                    else {
                        val c1 = i1.toChar()
                        when (c1) {
                            '\\' -> {
                                val i2 = reader.read()
                                if (i2 < 0) state = Error
                                else {
                                    val c2 = i2.toChar()
                                    when (c2) {
                                        '"' -> sb2.append('"')
                                        else -> sb2.append('\\').append(c2)
                                    }
                                }
                            }
                            '"' -> {
                                builder.addValue(sb.toString(), sb2.toString())
                                sb.setLength(0)
                                sb2.setLength(0)
                                val i2 = reader.read()
                                if (i2 < 0) state = End
                                else when (i2.toChar()) {
                                    '\n' -> state = End
                                    ',' -> state = FieldKey
                                    ' ' -> state = Timestamp
                                    else -> state = Error
                                }
                            }
                            else -> sb2.append(c1)
                        }
                    }
                }
                NonStringFieldValue -> {
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

                        state = End
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
                                state = if (',' == c1) FieldKey else Timestamp
                            }
                            else -> sb2.append(c1)
                        }
                    }
                }

                Timestamp -> {
                    val i1 = reader.read()
                    if (i1 < 0) {
                        builder.timestamp = parseLong(sb.toString())
                        state = End
                    } else {
                        val c1 = i1.toChar()
                        when (c1) {
                            '\n' -> {
                                builder.timestamp = parseLong(sb.toString())
                                state = End
                            }
                            else -> {
                                sb.append(c1)
                            }
                        }
                    }

                }
            }
        }

        return builder.build()

    }

}