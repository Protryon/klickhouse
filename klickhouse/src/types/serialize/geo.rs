use crate::{io::ClickhouseWrite, values::Value, Result};

use super::{Serializer, SerializerState, Type};

pub struct PointSerializer;

#[async_trait::async_trait]
impl Serializer for PointSerializer {
    async fn write_prefix<W: ClickhouseWrite>(
        _type_: &Type,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        for _ in 0..2 {
            Type::Float64.serialize_prefix(writer, state).await?;
        }
        Ok(())
    }

    async fn write<W: ClickhouseWrite>(
        _type_: &Type,
        values: Vec<Value>,
        writer: &mut W,
        state: &mut SerializerState,
    ) -> Result<()> {
        let mut columns = vec![Vec::with_capacity(values.len()); 2];
        for value in values {
            let Value::Point(point) = value else {
                unreachable!()
            };
            for (i, col) in columns.iter_mut().enumerate() {
                col.push(Value::Float64(point.0[i]));
            }
        }
        for column in columns {
            Type::Float64
                .serialize_column(column, writer, state)
                .await?;
        }
        Ok(())
    }
}

macro_rules! array_ser {
    ($name:ident, $item:ty) => {
        paste::paste! {
            pub struct [<$name Serializer>];
            impl super::array::ArraySerializerGeneric for [<$name Serializer>] {
                fn inner_type(_type_: &Type) -> &Type {
                    &Type::$item
                }
                fn value_len(value: &Value) -> usize {
                    match value {
                        Value::$name(array) => array.0.len(),
                        _ => unreachable!()
                    }
                }
                fn values(value: Value) -> Vec<Value> {
                    match value {
                        // The into_iter/collect is annoying, but unavoidable if we want
                        // to give strong types to the user inside the containers rather than
                        // [Value]s.
                        Value::$name(array) => array.0.into_iter().map(Value::$item).collect(),
                        _ => unreachable!()
                    }
                }
            }
        }
    };
}

array_ser!(Ring, Point);
array_ser!(Polygon, Ring);
array_ser!(MultiPolygon, Polygon);
