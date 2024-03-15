"use client";

import InternalCard from './Card';
import Grid from './Grid';
import Meta from './Meta';
const Card = InternalCard;
Card.Grid = Grid;
Card.Meta = Meta;
if (process.env.NODE_ENV !== 'production') {
  Card.displayName = 'Card';
}
export default Card;