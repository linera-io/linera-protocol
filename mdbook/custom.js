var localAddrs = ["localhost", "127.0.0.1", ""];

// make sure we don't activate google analytics if the developer is
// inspecting the book locally...
if (localAddrs.indexOf(document.location.hostname) === -1) {

    let script = document.createElement("script");
    script.src = "https://www.googletagmanager.com/gtag/js?id=G-L0N9LPFQ32";
    document.body.appendChild(script);

    window.dataLayer = window.dataLayer || [];
    function gtag(){dataLayer.push(arguments);}
    gtag('js', new Date());
    gtag('config', 'G-L0N9LPFQ32');
}
