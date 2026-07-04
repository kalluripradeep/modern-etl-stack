import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

/**
 * HTTP Basic Auth for the whole dashboard, including /api/chat.
 *
 * Enabled by setting DASHBOARD_AUTH_PASSWORD (and optionally
 * DASHBOARD_AUTH_USER, default "admin") in the environment. When unset,
 * the dashboard stays open — sensible for local development, but set a
 * password on any shared or cluster deployment.
 */
export function proxy(request: NextRequest) {
  const expectedPassword = process.env.DASHBOARD_AUTH_PASSWORD;
  if (!expectedPassword) {
    return NextResponse.next();
  }
  const expectedUser = process.env.DASHBOARD_AUTH_USER || 'admin';

  const header = request.headers.get('authorization') || '';
  if (header.startsWith('Basic ')) {
    try {
      const [user, ...rest] = atob(header.slice(6)).split(':');
      if (user === expectedUser && rest.join(':') === expectedPassword) {
        return NextResponse.next();
      }
    } catch {
      // fall through to 401 on malformed base64
    }
  }

  return new NextResponse('Authentication required', {
    status: 401,
    headers: { 'WWW-Authenticate': 'Basic realm="ETL Dashboard"' },
  });
}

export const config = {
  // Everything except Next.js internals and static assets
  matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'],
};
