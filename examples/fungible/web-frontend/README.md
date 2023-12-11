# Getting Started with the Fungible App.

This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app).

The application (contract/service) must be deployed on the network, and a Node Service must be running
on port 8080.

Web UIs for specific accounts can be opened by navigating URLs of the form
`http://localhost:3000/$CHAIN?app=$APP_ID&owner=$OWNER&port=$PORT` where
- the path is the ID of the chain where the account is located.
- the argument `app` is the token application ID obtained when creating the token.
- `owner` is the address of the chosen user account (owner must be have permissions to create blocks in the given chain).
- `port` is the port of the wallet service (the wallet must know the secret key of `owner`).

## Available Scripts

In the project directory, you can run:

### `npm start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in your browser.

The page will reload when you make changes.\
You may also see any lint errors in the console.

### `npx prettier --write .`

Formats the project using `prettier`.

### `npm run build`

Builds the app for production to the `build` folder.\
It bundles React in production mode and optimizes the build for the best performance.

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
