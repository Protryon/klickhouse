use crate::{io::ClickhouseRead, values::Value, Result};

use super::{Deserializer, DeserializerState, Type};

use crate::values;

pub struct PointDeserializer;

#[async_trait::async_trait]
impl Deserializer for PointDeserializer {
    async fn read_prefix<R: ClickhouseRead>(
        _type_: &Type,
        reader: &mut R,
        state: &mut DeserializerState,
    ) -> Result<()> {
        for _ in 0..2 {
            Type::Float64.deserialize_prefix(reader, state).await?;
        }
        Ok(())
    }

    async fn read<R: ClickhouseRead>(
        _type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState,
    ) -> Result<Vec<Value>> {
        let mut points = vec![Value::Point(Default::default()); rows];
        for col in 0..2 {
            for (row, value) in Type::Float64
                .deserialize_column(reader, rows, state)
                .await?
                .into_iter()
                .enumerate()
            {
                let Value::Float64(value) = value else {
                    unreachable!()
                };
                match &mut points[row] {
                    Value::Point(point) => point.0[col] = value,
                    _ => {
                        unreachable!()
                    }
                }
            }
        }
        Ok(points)
    }
}
macro_rules! array_deser {
    ($name:ident, $item:ty) => {
        paste::paste! {
            pub struct [<$name Deserializer>];
            impl super::array::ArrayDeserializerGeneric for [<$name Deserializer>] {
                type Item = crate::values::$item;
                fn inner_type(_type_: &Type) -> &Type {
                    &Type::$item
                }
                fn inner_value(items: Vec<Self::Item>) -> Value {
                    Value::$name(values::$name(items))
                }
                fn item_mapping(value: Value) -> Self::Item {
                    let Value::$item(point) = value else {
                        unreachable!()
                    };
                    point
                }
            }
        }
    };
}

array_deser!(Ring, Point);
array_deser!(Polygon, Ring);
array_deser!(MultiPolygon, Polygon);
