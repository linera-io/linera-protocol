use std::iter::IntoIterator;

pub type ResultWith<X, T, E> = Result<(T, X), (E, X)>;

pub trait AggregateExt {
    type Item;
    type Output;

    fn aggregate(self, target: &mut impl Extend<Self::Item>) -> Self::Output;
}

impl<R, Xs: IntoIterator> AggregateExt for (R, Xs) {
    type Item = Xs::Item;
    type Output = R;

    fn aggregate(self, target: &mut impl Extend<Self::Item>) -> Self::Output {
        target.extend(self.1);
        self.0
    }
}

pub trait TryAggregateExt: AggregateExt {
    type Ok;
    type Err;

    fn try_aggregate<Target: Default + Extend<Self::Item>>(self, target: &mut Target) -> Result<Self::Ok, (Self::Err, Target)>;
}

impl<Xs: IntoIterator, T, E> TryAggregateExt for (Result<T, E>, Xs) {
    type Ok = T;
    type Err = E;

    fn try_aggregate<Target: Default + Extend<Self::Item>>(self, target: &mut Target) -> Result<Self::Ok, (Self::Err, Target)> {
        target.extend(self.1);
        match self.0 {
            Ok(x) => Ok(x),
            Err(e) => Err((e, std::mem::replace(target, Default::default()))),
        }
    }
}


pub trait DistributeExt {
    type Ok;
    type Err;
    type Scalar;

    fn distribute<E_: From<Self::Err>>(self) -> Result<(Self::Ok, Self::Scalar), (E_, Self::Scalar)>;
}

impl<T, E, X> DistributeExt for (Result<T, E>, X) {
    type Ok = T;
    type Err = E;
    type Scalar = X;

    fn distribute<E_: From<E>>(self) -> ResultWith<X, T, E_> {
        match self.0 {
            Ok(x) => Ok((x, self.1)),
            Err(e) => Err((e.into(), self.1)),
        }
    }
}


pub trait FactorExt {
    type Ok;
    type Err;
    type Scalar;

    fn factor(self) -> (Result<Self::Ok, Self::Err>, Self::Scalar);
}

impl<T, E, X> FactorExt for ResultWith<X, T, E> {
    type Ok = T;
    type Err = E;
    type Scalar = X;

    fn factor(self) -> (Result<T, E>, X) {
        match self {
            Ok((x, scalar)) => (Ok(x), scalar),
            Err((e, scalar)) => (Err(e), scalar),
        }
    }
}
