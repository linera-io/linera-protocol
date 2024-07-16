# Getting Started with the Social App.

This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app).

The application (contract/service) must be deployed on the network, and a Node Service must be running
on port 8000. Also make sure you have performed the operations mentoined in this [README](../README.md)

### Note

The app is an example of a social app built using linera, and will be improved with future updates. Users can see their post by `ownPost` query in GraphiQL IDE.

#### Operations avaiable:

Post, Likes, and Comments (only by the creation chain).

To access the social app for a given Application Id, append the Application Id and port to the URI as so:

```
http://localhost:3000/7817752ff06b8266d77df8febf5c4b524cec096bd83dc54f989074fb94f833737ae984f32be2cee1dfab766fe2d0c726503c4d97117eb59023e9cc65a8ecd1f7000000000000000000000000000000007817752ff06b8266d77df8febf5c4b524cec096bd83dc54f989074fb94f833737ae984f32be2cee1dfab766fe2d0c726503c4d97117eb59023e9cc65a8ecd1f702000000000000000000000000000000&port=8000
```

## Available Scripts

In the project directory, you can run:

### `npm run tailwind`

Runs the tailwind CLI build process

Open [tailwindcss](https://tailwindcss.com/docs/installation) to learn more about tailwindcss.

### `npm start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in your browser.

The page will reload when you make changes.\
You may also see any lint errors in the console.

### `npx prettier --write .`

Formats the project using `prettier`.

### `npm run build`

Builds the app for production to the `build` folder.\
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.\
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

### `npm run eject`

**Note: this is a one-way operation. Once you `eject`, you can't go back!**

If you aren't satisfied with the build tool and configuration choices, you can `eject` at any time. This command will remove the single build dependency from your project.

Instead, it will copy all the configuration files and the transitive dependencies (webpack, Babel, ESLint, etc) right into your project so you have full control over them. All of the commands except `eject` will still work, but they will point to the copied scripts so you can tweak them. At this point you're on your own.

You don't have to ever use `eject`. The curated feature set is suitable for small and middle deployments, and you shouldn't feel obligated to use this feature. However we understand that this tool wouldn't be useful if you couldn't customize it when you are ready for it.

## Learn More

To learn React, check out the [React documentation](https://reactjs.org/).
