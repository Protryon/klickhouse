//! Geo types
//! <https://clickhouse.com/docs/en/sql-reference/data-types/geo>
use super::*;

#[derive(Clone, Default, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Geo point, represented by its x and y coordinates.
///
/// <https://clickhouse.com/docs/en/sql-reference/data-types/geo#point>
pub struct Point(pub [f64; 2]);
impl std::hash::Hash for Point {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for x in self.0 {
            x.to_bits().hash(state);
        }
    }
}
impl std::ops::Index<u8> for Point {
    type Output = f64;
    fn index(&self, index: u8) -> &Self::Output {
        &self.0[index as usize]
    }
}
impl AsRef<[f64; 2]> for Point {
    fn as_ref(&self) -> &[f64; 2] {
        &self.0
    }
}
#[derive(Clone, Hash, Default, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Polygon without holes.
///
/// <https://clickhouse.com/docs/en/sql-reference/data-types/geo#ring>
pub struct Ring(pub Vec<Point>);
#[derive(Clone, Hash, Default, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Polygon with holes. The first element is the outer polygon, and the following ones are the holes.
///
/// <https://clickhouse.com/docs/en/sql-reference/data-types/geo#polygon>
pub struct Polygon(pub Vec<Ring>);
#[derive(Clone, Hash, Default, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Union of polygons.
///
/// <https://clickhouse.com/docs/en/sql-reference/data-types/geo#multipolygon>
pub struct MultiPolygon(pub Vec<Polygon>);

macro_rules! to_from_sql {
    ($name:ident) => {
        impl ToSql for $name {
            fn to_sql(self, _type_hint: Option<&Type>) -> Result<Value> {
                Ok(Value::$name(self))
            }
        }

        impl FromSql for $name {
            fn from_sql(type_: &Type, value: Value) -> Result<Self> {
                if !matches!(type_, Type::$name) {
                    return Err(unexpected_type(type_));
                }
                match value {
                    Value::$name(x) => Ok(x),
                    _ => unimplemented!(),
                }
            }
        }
    };
}

to_from_sql!(Point);
to_from_sql!(Ring);
to_from_sql!(Polygon);
to_from_sql!(MultiPolygon);
