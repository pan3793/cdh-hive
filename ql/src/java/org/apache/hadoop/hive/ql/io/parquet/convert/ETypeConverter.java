package org.apache.hadoop.hive.ql.io.parquet.convert;

import java.math.BigDecimal;

import org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable;
import org.apache.hadoop.hive.ql.io.parquet.writable.BinaryWritable.DicBinaryWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import parquet.column.Dictionary;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.PrimitiveConverter;

/**
 *
 * ETypeConverter is an easy way to set the converter for the right type.
 *
 */
public enum ETypeConverter {

  EDOUBLE_CONVERTER(Double.TYPE) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new PrimitiveConverter() {
        @Override
        final public void addDouble(final double value) {
          parent.set(index, new DoubleWritable(value));
        }
      };
    }
  },
  EBOOLEAN_CONVERTER(Boolean.TYPE) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new PrimitiveConverter() {
        @Override
        final public void addBoolean(final boolean value) {
          parent.set(index, new BooleanWritable(value));
        }
      };
    }
  },
  EFLOAT_CONVERTER(Float.TYPE) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new PrimitiveConverter() {
        @Override
        final public void addFloat(final float value) {
          parent.set(index, new FloatWritable(value));
        }
      };
    }
  },
  EINT32_CONVERTER(Integer.TYPE) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new PrimitiveConverter() {
        @Override
        final public void addInt(final int value) {
          parent.set(index, new IntWritable(value));
        }
      };
    }
  },
  EINT64_CONVERTER(Long.TYPE) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new PrimitiveConverter() {
        @Override
        final public void addLong(final long value) {
          parent.set(index, new LongWritable(value));
        }
      };
    }
  },
  EINT96_CONVERTER(BigDecimal.class) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new PrimitiveConverter() {
        @Override
        final public void addDouble(final double value) {
          parent.set(index, new DoubleWritable(value));
        }
      };
    }
  },
  EBINARY_CONVERTER(Binary.class) {
    @Override
    Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
      return new PrimitiveConverter() {
        private Binary[] dictBinary;
        private String[] dict;

        @Override
        public boolean hasDictionarySupport() {
          return true;
        }

        @Override
        public void setDictionary(Dictionary dictionary) {
          dictBinary = new Binary[dictionary.getMaxId() + 1];
          dict = new String[dictionary.getMaxId() + 1];
          for (int i = 0; i <= dictionary.getMaxId(); i++) {
            Binary binary = dictionary.decodeToBinary(i);
            dictBinary[i] = binary;
            dict[i] = binary.toStringUsingUTF8();
          }
        }

        @Override
        public void addValueFromDictionary(int dictionaryId) {
          parent.set(index, new DicBinaryWritable(dictBinary[dictionaryId],  dict[dictionaryId]));
        }

        @Override
        final public void addBinary(Binary value) {
          parent.set(index, new BinaryWritable(value));
        }
      };
    }
  };
  final Class<?> _type;

  private ETypeConverter(final Class<?> type) {
    this._type = type;
  }

  private Class<?> getType() {
    return _type;
  }

  abstract Converter getConverter(final Class<?> type, final int index, final HiveGroupConverter parent);

  static public Converter getNewConverter(final Class<?> type, final int index, final HiveGroupConverter parent) {
    for (final ETypeConverter eConverter : values()) {
      if (eConverter.getType() == type) {
        return eConverter.getConverter(type, index, parent);
      }
    }
    throw new RuntimeException("Converter not found ... for type : " + type);
  }

}
