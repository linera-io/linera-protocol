use std::iter::IntoIterator;

pub type ResultWith<X, T, E> = Result<(T, X), (E, X)>;

pub trait AggregateExt: IntoIterator {
    fn aggregate<Ts: IntoIterator<Item = T>, R>(&mut self, (output, items): (R, Ts)) -> R {
        self.extend(items);
        output
    }

    fn try_aggregate<Ts: IntoIterator<Item = T>, R, E>(&mut self, (output, items): (Result<R, E>, Ts)) -> Result<R, (E, Self)> where Self: Sized + Default {
        self.extend(items);
        match output {
            Ok(x) => Ok(x),
            Err(e) => Err((e, std::mem::replace(self, Default::default()))),
        }
    }
}

impl<U, T: Extend<U>> AggregateExt<U> for T { }


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
