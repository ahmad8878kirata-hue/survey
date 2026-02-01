# Survey website + email delivery (passwordless)

The forms send submissions **directly to FormSubmit** from the browser, so you can receive emails **without SMTP, passwords, or app-passwords**.

## Set your receiving email

In the HTML files (`استبيان عمال.html`, `استبيان مدراء.html`) the receiving inbox is set here:

- `window.SURVEY_RECEIVER_EMAIL` (optional override)
- otherwise it uses the hardcoded default email in the script

Example (put before the form script, or run in console):

```html
<script>
  window.SURVEY_RECEIVER_EMAIL = "your@email.com";
</script>
```

## Important: first-time activation

If it’s your first time using an email with FormSubmit, you must **click the activation link** sent to your inbox (check SPAM too). After activation, submissions will deliver normally.

## Local usage

You can open the HTML files directly, or serve them locally. If you want a local server:

```bash
npm install
npm start
```

Then open:
- `http://localhost:3000/`
