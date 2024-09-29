# wpcom.ex

Official Elixir library for the WordPress.com REST API.

## Setup

```elixir
def deps do
  [
    {:wpcom, "~> 1.0.0"}
  ]
end
```

### Authentication

Some WordPress.com REST API endpoints are publicly accessible. For instance, retrieving a list of posts from a public site can be done without any credentials.

However, many API operations do require authentication. So if your application needs to access protected endpoints, you'll need to obtain an OAuth2 token. For detailed instructions on acquiring a token, refer to the [OAuth2 Authentication guide](https://developer.wordpress.com/docs/oauth2/) in the WordPress.com Developer Resources.

Once you've obtained your token, you can configure your application to use it by adding the following line to your `runtime.exs` file:

```elixir
config :wpcom, :oauth2_token: "_YOUR_OAUTH2_TOKEN_HERE_"
```

## Testing

```
mix test
```

## Static Analysis

```
mix dialyzer
```

```
mix credo
```
