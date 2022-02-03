+++
title = "Vercel"
weight = 50
+++

Vercel (previously zeit) is similar to Netlify, making deployment of sites easy.
The sites are hosted by Vercel and automatically deployed whenever we push a
commit to our selected production branch (e.g, master).

If you don't have an account with Vercel, you can sign up [here](https://vercel.com/signup).

## Automatic deploys

Once you sign up you can import your site from a Git provider (Github, GitLab or Bitbucket). 
After the import, you can set the settings for your project. 

- Choose Framework Preset as **Other**
- Build command as `zola build` and make sure toggle on Override switch.
- By default Vercel chooses output directory as `public`, if you use a different directory then specify output directory.
- To add your own domain, go to domain setting in left and add it there.


All we have to is include a `vercel.json` in our project's root directory by
specifying the `ZOLA_VERSION` we want to use to deploy the site.

```
  {
  "build": {
    "env": {
      "ZOLA_VERSION": "0.12.0"
    }
  }
}
```

And your site should now be up and running.
