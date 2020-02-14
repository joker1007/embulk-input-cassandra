package org.embulk.input.cassandra.writers;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.TypeTokens;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.msgpack.value.ValueFactory.MapBuilder;

@SuppressWarnings({"UnstableApiUsage", "rawtypes", "unchecked"})
public abstract class CollectionColumnWriter extends ColumnWriter {
  private DataType cassandraDataType;

  public CollectionColumnWriter(int columnIndex, DataType cassandraDataType) {
    super(columnIndex);
    this.cassandraDataType = cassandraDataType;
  }

  protected List<TypeToken> getElementTypeTokens() {
    switch (cassandraDataType.getName()) {
      case LIST:
      case SET:
        DataType elementType = cassandraDataType.getTypeArguments().get(0);
        return ImmutableList.of(getTypeToken(elementType));
      case MAP:
        DataType keyElementType = cassandraDataType.getTypeArguments().get(0);
        DataType valueElementType = cassandraDataType.getTypeArguments().get(1);
        return ImmutableList.of(getTypeToken(keyElementType), getTypeToken(valueElementType));
      default:
        throw new RuntimeException("Unsupported cassandra data type");
    }
  }

  private TypeToken getTypeToken(DataType cassandraDataType) {
    switch (cassandraDataType.getName()) {
      case LIST:
        return getTypeTokenForList(cassandraDataType);
      case SET:
        return getTypeTokenForSet(cassandraDataType);
      case MAP:
        return getTypeTokenForMap(cassandraDataType);
      case VARCHAR:
      case TEXT:
      case ASCII:
        return TypeToken.of(String.class);
      case DECIMAL:
        return TypeToken.of(BigDecimal.class);
      case COUNTER:
        return TypeToken.of(Long.class);
      case SMALLINT:
        return TypeToken.of(Short.class);
      case TINYINT:
        return TypeToken.of(Byte.class);
      case INT:
        return TypeToken.of(Integer.class);
      case BIGINT:
      case TIME:
        return TypeToken.of(Long.class);
      case VARINT:
        return TypeToken.of(BigInteger.class);
      case INET:
        return TypeToken.of(InetAddress.class);
      case UUID:
      case TIMEUUID:
        return TypeToken.of(UUID.class);
      case BOOLEAN:
        return TypeToken.of(Boolean.class);
      case TIMESTAMP:
        return TypeToken.of(Date.class);
      case DATE:
        return TypeToken.of(LocalDate.class);
      case FLOAT:
        return TypeToken.of(Float.class);
      case DOUBLE:
        return TypeToken.of(Double.class);
      default:
        throw new RuntimeException("Unsupported cassandra type");
    }
  }

  private TypeToken getTypeTokenForList(DataType listType) {
    return TypeTokens.listOf(getTypeToken(listType.getTypeArguments().get(0)));
  }

  private TypeToken getTypeTokenForSet(DataType setType) {
    return TypeTokens.listOf(getTypeToken(setType.getTypeArguments().get(0)));
  }

  private TypeToken getTypeTokenForMap(DataType mapType) {
    List<DataType> typeArguments = mapType.getTypeArguments();
    TypeToken keyType = getTypeToken(typeArguments.get(0));
    TypeToken valueType = getTypeToken(typeArguments.get(1));
    return TypeTokens.mapOf(keyType, valueType);
  }

  protected Value convertListToMsgPack(List<Object> list) {
    if (list == null) {
      return ValueFactory.newNil();
    }

    return ValueFactory.newArray(
        list.stream().map(this::javaValueToMsgPack).collect(Collectors.toList()));
  }

  protected Value convertSetToMsgPack(Set<Object> set) {
    if (set == null) {
      return ValueFactory.newNil();
    }

    return ValueFactory.newArray(
        set.stream().map(this::javaValueToMsgPack).collect(Collectors.toList()));
  }

  protected Value convertMapToMsgPack(Map<Object, Object> map) {
    if (map == null) {
      return ValueFactory.newNil();
    }

    MapBuilder mapBuilder = ValueFactory.newMapBuilder();
    map.forEach((key, value) -> {
      Value keyValue = javaValueToMsgPack(key);
      Value valueValue = javaValueToMsgPack(value);
      mapBuilder.put(keyValue, valueValue);
    });
    return mapBuilder.build();
  }

  private Value javaValueToMsgPack(Object value) {
    if (value == null) {
      return ValueFactory.newNil();
    }

    if (value instanceof String) {
      return ValueFactory.newString((String) value);
    } else if (value instanceof Byte) {
      return ValueFactory.newInteger((Byte) value);
    } else if (value instanceof Integer) {
      return ValueFactory.newInteger((Integer) value);
    } else if (value instanceof Long) {
      return ValueFactory.newInteger((Long) value);
    } else if (value instanceof BigDecimal) {
      return ValueFactory.newString(((BigDecimal) value).toPlainString());
    } else if (value instanceof InetAddress) {
      return ValueFactory.newString(((InetAddress) value).getHostAddress());
    } else if (value instanceof BigInteger) {
      return ValueFactory.newInteger(((BigInteger) value).longValue());
    } else if (value instanceof Boolean) {
      return ValueFactory.newBoolean((Boolean) value);
    } else if (value instanceof Float) {
      return ValueFactory.newFloat((Float) value);
    } else if (value instanceof Double) {
      return ValueFactory.newFloat((Double) value);
    } else if (value instanceof Date) {
      return ValueFactory.newInteger(((Date) value).toInstant().toEpochMilli());
    } else if (value instanceof LocalDate) {
      return ValueFactory.newInteger(((LocalDate) value).getMillisSinceEpoch());
    } else if (value instanceof UUID) {
      return ValueFactory.newString(value.toString());
    } else if (value instanceof List) {
      return convertListToMsgPack((List<Object>) value);
    } else if (value instanceof Set) {
      return convertSetToMsgPack((Set<Object>) value);
    } else if (value instanceof Map) {
      return convertMapToMsgPack((Map<Object, Object>) value);
    } else {
      throw new RuntimeException("Unsupported cassandra type");
    }
  }
}
